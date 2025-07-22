import json
import logging
import re
import tempfile
from math import ceil
from typing import List, Optional, Tuple
from urllib.parse import urljoin, parse_qs, urlparse
from dataclasses import asdict

import bencodepy
import cloudscraper
import redis
import lxml.html
from concurrent.futures import ThreadPoolExecutor
from rapidfuzz import fuzz
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

# Подробное логирование HTTP-запросов
logging.getLogger("urllib3.connectionpool").setLevel(logging.DEBUG)
logging.getLogger("cloudscraper").setLevel(logging.DEBUG)
log = logging.getLogger(__name__)

# Регулярные выражения
_LOSSLESS_RE    = re.compile(r"\b(flac|wavpack|wv|ape|alac|aiff|pcm|dts|mlp|tta|mqa|lossless)\b", re.IGNORECASE)
_LOSSY_RE       = re.compile(r"\b(mp3|aac|ogg|opus|lossy)\b", re.IGNORECASE)
_TOTAL_RE       = re.compile(r"Результатов поиска:\s*(\d+)", re.IGNORECASE)
_PG_BASE_URL_RE = re.compile(r"PG_BASE_URL\s*:\s*['\"][^?]+\?([^'\"]+)['\"]", re.IGNORECASE)

def _parse_filelist(torrent_bytes: bytes) -> List[str]:
    meta = bencodepy.decode(torrent_bytes)
    info = meta[b"info"]
    if b"filename" in info:
        return [info[b"filename"].decode(errors="ignore")]
    return [
        "/".join(p.decode(errors="ignore") for p in f[b"path"])
        for f in info.get(b"files", [])
    ]

def _contains_track(filelist: List[str], track: str) -> bool:
    t = track.lower()
    return any(t in fname.lower() or fuzz.partial_ratio(t, fname.lower()) >= 80 for fname in filelist)

class RutrackerService:
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = (base_url or settings.rutracker_base).rstrip("/")
        self.scraper = cloudscraper.create_scraper(
            interpreter="js2py",
            delay=10,
            browser={"browser": "chrome", "platform": "windows"},
            debug=False
        )
        self.redis = redis.Redis.from_url(settings.redis_url)
        self.cookie_ttl = getattr(settings, "rutracker_cookie_ttl", 86400)
        try:
            raw = self.redis.get("rutracker:cookiejar")
            if raw:
                ck = json.loads(raw)
                self.scraper.cookies.update(ck)
                log.debug("Loaded cookies from Redis: %s", ck)
        except Exception:
            log.exception("Failed to load cookies from Redis")

    def _ensure_login(self):
        url = f"{self.base_url}/forum/tracker.php"
        log.debug("GET %s (check login)", url)
        r = self.scraper.get(url, allow_redirects=False)
        log.debug("Status %s, headers %s", r.status_code, r.headers)
        if r.is_redirect and "login.php" in r.headers.get("Location", ""):
            log.info("Session expired, re-authenticating...")
            self.redis.delete("rutracker:cookiejar")
            self._login_sync()
        else:
            log.debug("Session valid")

    def _login_with_selenium(self):
        login_url = f"{self.base_url}/forum/login.php"
        options = Options()
        options.headless = True
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        driver = webdriver.Chrome(options=options)
        try:
            log.debug("Selenium: opening login page %s", login_url)
            driver.get(login_url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "login_username"))
            )
            driver.find_element(By.NAME, "login_username").send_keys(settings.rutracker_login)
            driver.find_element(By.NAME, "login_password").send_keys(settings.rutracker_password)
            driver.find_element(By.XPATH, "//input[@type='submit']").click()
            WebDriverWait(driver, 15).until(
                EC.url_contains("tracker.php")
            )
            cookies = driver.get_cookies()
            ck = {c["name"]: c["value"] for c in cookies}
            self.scraper.cookies.update(ck)
            self.redis.set("rutracker:cookiejar", json.dumps(ck), ex=self.cookie_ttl)
            log.debug("Selenium login successful, cookies: %s", ck)
        finally:
            driver.quit()

    def _login_sync(self):
        login_url = f"{self.base_url}/forum/login.php"
        log.debug("GET %s", login_url)
        r = self.scraper.get(login_url)
        r.raise_for_status()

        if "captcha" in r.text.lower():
            log.info("CAPTCHA detected, falling back to Selenium")
            self._login_with_selenium()
            return

        doc = lxml.html.fromstring(r.text)
        form = next(
            (f for f in doc.xpath("//form") if "login.php" in f.get("action", "")),
            None
        )
        if form is None:
            raise RuntimeError("Login form not found")

        raw_act = form.get("action") or login_url
        post_url = raw_act if raw_act.startswith("http") else urljoin(login_url, raw_act)
        sep = "&" if "?" in post_url else "?"
        post_url = f"{post_url}{sep}login_try=Y"

        data = {inp.get("name"): inp.get("value", "") for inp in form.xpath(".//input[@name]")}
        data["login_username"] = settings.rutracker_login
        data["login_password"] = settings.rutracker_password

        post = self.scraper.post(
            post_url,
            data=data,
            headers={"Referer": login_url},
            allow_redirects=True
        )
        post.raise_for_status()

        cookies = self.scraper.cookies.get_dict()
        if not (cookies.get("bb_session") or cookies.get("bb_sessionhash")):
            raise RuntimeError("Login failed: session cookie not found")

        self.redis.set("rutracker:cookiejar", json.dumps(cookies), ex=self.cookie_ttl)

    def _search_sync(self, query: str, only_lossless: Optional[bool], track: Optional[str]) -> List[TorrentInfo]:
        cache_key = f"search:{query}:{only_lossless}:{track}"
        if (raw := self.redis.get(cache_key)):
            return [TorrentInfo(**r) for r in json.loads(raw)]

        self._ensure_login()
        r0 = self.scraper.get(f"{self.base_url}/forum/tracker.php", params={"nm": query})
        r0.raise_for_status()
        doc0 = lxml.html.fromstring(r0.text)

        form = doc0.get_element_by_id("tr-form")
        action = form.action or f"{self.base_url}/forum/tracker.php"
        if not action.startswith("http"):
            action = urljoin(self.base_url + "/forum/", action.lstrip("/"))

        post_data = {i.name: i.value for i in form.xpath(".//input[@type='hidden']")}
        post_data.update({"nm": query, "f[]": "-1"})

        r1 = self.scraper.post(action, data=post_data)
        r1.raise_for_status()
        doc1 = lxml.html.fromstring(r1.text)

        total = int(_TOTAL_RE.search(doc1.text_content()).group(1)) if _TOTAL_RE.search(doc1.text_content()) else 0
        per_page = 50
        pages = ceil(total / per_page) or 1

        search_id = None
        for script in doc1.xpath("//script/text()"):
            if m := _PG_BASE_URL_RE.search(script):
                search_id = parse_qs(m.group(1)).get("search_id", [None])[0]
                break

        parsed: List[Tuple[TorrentInfo, int]] = []
        def parse_page(doc):
            for row in doc.xpath("//tr[@data-topic_id]"):
                try:
                    href = row.xpath(".//a[contains(@href,'dl.php?t=')]/@href")[0]
                    tid = int(parse_qs(urlparse(href).query)["t"][0])
                    forum_txt = " ".join(row.xpath(".//td[contains(@class,'f-name-col')]//a/text()"))
                    title = row.xpath(".//td[contains(@class,'t-title-col')]//a/text()")[0]
                    combined = f"{forum_txt} {title}"
                    is_l = bool(_LOSSLESS_RE.search(combined))
                    is_y = bool(_LOSSY_RE.search(combined))
                    if only_lossless is True and (not is_l or is_y): continue
                    if only_lossless is False and is_l: continue
                    size = row.xpath(".//a[contains(@href,'dl.php?t=')]/text()")[0].replace("\xa0", " ").strip()
                    se = int(row.xpath(".//b[contains(@class,'seedmed')]/text()")[0] or 0)
                    le = int(row.xpath(".//td[contains(@class,'leechmed')]/text()")[0] or 0)
                    parsed.append((TorrentInfo(title=title,
                                               url=urljoin(self.base_url+"/forum/", href),
                                               size=size,
                                               seeders=se,
                                               leechers=le), tid))
                except Exception:
                    continue

        parse_page(doc1)
        if pages > 1 and search_id:
            for off in range(1, pages):
                html = self.scraper.get(
                    f"{self.base_url}/forum/tracker.php",
                    params={"search_id": search_id, "start": off * per_page}
                ).text
                parse_page(lxml.html.fromstring(html))

        if track:
            filtered = []
            for ti, tid in parsed:
                key2 = f"tracklist:{tid}"
                raw2 = self.redis.get(key2)
                if raw2:
                    fl = json.loads(raw2)
                else:
                    data = self._download_sync(tid)
                    fl = _parse_filelist(data)
                    self.redis.set(key2, json.dumps(fl), ex=3600)
                if _contains_track(fl, track):
                    filtered.append(ti)
            return filtered

        results = [ti for ti, _ in parsed]
        self.redis.set(cache_key, json.dumps([asdict(r) for r in results]), ex=300)
        return results

    async def search(self, query: str, only_lossless: Optional[bool] = None, track: Optional[str] = None) -> List[TorrentInfo]:
        import asyncio
        return await asyncio.to_thread(self._search_sync, query, only_lossless, track)

    def _download_sync(self, topic_id: int) -> bytes:
        self._ensure_login()
        dl_url = f"{self.base_url}/forum/dl.php?t={topic_id}"
        resp = self.scraper.get(dl_url, allow_redirects=False)
        if resp.is_redirect and "login.php" in resp.headers.get("Location", ""):
            self._login_sync()
            resp = self.scraper.get(dl_url, allow_redirects=True)
        resp.raise_for_status()
        if b"announce" not in resp.content:
            raise ValueError("Downloaded file is not a valid .torrent")
        return resp.content

    async def download(self, topic_id: int) -> bytes:
        import asyncio
        return await asyncio.to_thread(self._download_sync, topic_id)

    async def close(self):
        pass