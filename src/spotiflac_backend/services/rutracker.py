import json
import logging
import os
import re
import uuid
from math import ceil
from typing import List, Optional, Tuple
from urllib.parse import urljoin, parse_qs, urlparse

import bencodepy
import cloudscraper
import lxml.html
import redis
from rapidfuzz import fuzz

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

log = logging.getLogger(__name__)

# Регэкспы и токены
_LOSSLESS_RE    = re.compile(r"\b(flac|wavpack|wv|ape|alac|aiff|pcm|dts|mlp|tta|mqa|lossless)\b", re.IGNORECASE)
_LOSSY_RE       = re.compile(r"\b(mp3|aac|ogg|opus|lossy)\b", re.IGNORECASE)
_PG_BASE_URL_RE = re.compile(r"PG_BASE_URL\s*:\s*['\"][^?]+\?([^'\"]+)['\"]", re.IGNORECASE)
_TOTAL_RE       = re.compile(r"Результатов поиска:\s*(\d+)", re.IGNORECASE)
_FORM_TOKEN_RE  = re.compile(r"form_token\s*:\s*'([0-9a-f]+)'")

CAPTCHA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "screenshots")
os.makedirs(CAPTCHA_DIR, exist_ok=True)

class RutrackerService:
    def __init__(self, base_url: str = None):
        self.base_url = (base_url or settings.rutracker_base).rstrip("/")
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False},
            delay=10
        )
        self.redis = redis.Redis.from_url(settings.redis_url)
        self.cookie_ttl = getattr(settings, "rutracker_cookie_ttl", 24 * 3600)

        log.debug("Initializing RutrackerService with base_url=%s", self.base_url)
        try:
            raw = self.redis.get("rutracker:cookiejar")
            if raw:
                cookies = json.loads(raw)
                self.scraper.cookies.update(cookies)
                log.debug("Restored cookies: %s", self.scraper.cookies.get_dict())
        except Exception:
            log.exception("Failed to restore cookies")

    def _extract_form_token(self, html: str) -> str:
        m = _FORM_TOKEN_RE.search(html)
        token = m.group(1) if m else ""
        log.debug("Extracted form_token=%s", token)
        return token

    def initiate_login(self) -> Tuple[Optional[str], Optional[str]]:
        login_url = f"{self.base_url}/forum/login.php"
        resp = self.scraper.get(login_url); resp.raise_for_status()
        doc = lxml.html.fromstring(resp.text)

        # Найти капчу
        captcha_img = doc.xpath("//img[contains(@src,'/captcha/')]/@src")
        log.debug("Captcha img src: %s", captcha_img)
        if not captcha_img:
            log.debug("No CAPTCHA image on login page")
            return None, None

        # SID для капчи
        sid = parse_qs(urlparse(captcha_img[0]).query).get("sid", [None])[0]

        # Скрытые поля (redirect и другие)
        form = doc.get_element_by_id("login-form-full")
        hidden = {
            inp.get("name"): inp.get("value", "")
            for inp in form.xpath(".//input[@type='hidden']")
            if inp.get("name")
        }

        # Динамическое имя поля для кода капчи
        code_field = form.xpath(".//input[starts-with(@name,'cap_code_')]/@name")
        if not code_field:
            log.error("Captcha code field not found")
            raise RuntimeError("CAPTCHA_PARSE_ERROR")
        code_field = code_field[0]
        log.debug("Captcha code field name: %s", code_field)

        session_id = uuid.uuid4().hex
        self.redis.set(
            f"login:{session_id}",
            json.dumps({"sid": sid, "hidden": hidden, "code_field": code_field}),
            ex=300
        )

        cap_url = urljoin(self.base_url + "/forum/", captcha_img[0])
        img_data = self.scraper.get(cap_url).content
        img_path = os.path.join(CAPTCHA_DIR, f"captcha-{session_id}.png")
        with open(img_path, "wb") as f:
            f.write(img_data)
        log.debug("Downloaded CAPTCHA image to %s", img_path)

        raise RuntimeError(f"CAPTCHA_REQUIRED:{session_id}")

    def complete_login(self, session_id: str, solution: str):
        raw = self.redis.get(f"login:{session_id}")
        if not raw:
            log.error("No such captcha session %s", session_id)
            raise RuntimeError("Invalid or expired captcha session")
        info = json.loads(raw)
        sid, hidden, code_field = info["sid"], info["hidden"], info["code_field"]

        login_url = f"{self.base_url}/forum/login.php"
        resp = self.scraper.get(login_url); resp.raise_for_status()
        doc = lxml.html.fromstring(resp.text)
        submit_val = doc.xpath("//form[@id='login-form-full']//input[@type='submit']/@value")[0]

        data = hidden.copy()
        data.update({
            "login_username": settings.rutracker_login,
            "login_password": settings.rutracker_password,
            "cap_sid": sid,
            code_field: solution,
            "login": submit_val,
        })
        log.debug("Submitting CAPTCHA login with fields: %s", list(data.keys()))

        post = self.scraper.post(login_url, data=data, headers={"Referer": login_url})
        post.raise_for_status()
        if "top-user-login" not in post.text:
            log.error("Login after CAPTCHA failed")
            raise RuntimeError("Login failed after captcha")

        jar = self.scraper.cookies.get_dict()
        self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)
        log.debug("CAPTCHA login succeeded, cookies: %s", jar)

    def _login_sync(self):
        login_url = f"{self.base_url}/forum/login.php"
        resp = self.scraper.get(login_url); resp.raise_for_status()
        html = resp.text
        token = self._extract_form_token(html)

        doc = lxml.html.fromstring(html)
        form = doc.get_element_by_id("login-form-full")

        data = {
            inp.get("name"): inp.get("value", "")
            for inp in form.xpath(".//input[@type='hidden']")
            if inp.get("name")
        }
        if token:
            data["form_token"] = token

        submit_val = form.xpath(".//input[@type='submit']/@value")[0]
        data.update({
            "login_username": settings.rutracker_login,
            "login_password": settings.rutracker_password,
            "login": submit_val,
        })
        log.debug("Submitting standard login with fields: %s", list(data.keys()))

        post = self.scraper.post(login_url, data=data, headers={"Referer": login_url})
        post.raise_for_status()

        jar = self.scraper.cookies.get_dict()
        if not (jar.get("bb_session") or jar.get("bb_sessionhash")):
            log.warning("Standard login did not yield session cookie")
            raise RuntimeError("LOGIN_NO_SESSION")

        self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)
        log.debug("Standard login succeeded, cookies cached")

    def _ensure_login(self):
        jar = self.scraper.cookies.get_dict()
        if jar.get("bb_session") or jar.get("bb_sessionhash"):
            return

        try:
            self._login_sync()
        except RuntimeError as e:
            if str(e) == "LOGIN_NO_SESSION":
                log.info("Falling back to CAPTCHA due to missing session cookie")
                return self.initiate_login()
            if str(e).startswith("CAPTCHA_REQUIRED"):
                raise
            raise

    def _parse_filelist(self, torrent_bytes: bytes) -> List[str]:
        meta = bencodepy.decode(torrent_bytes)[b"info"]
        if b"filename" in meta:
            return [meta[b"filename"].decode(errors="ignore")]
        paths = []
        for f in meta.get(b"files", []):
            parts = [p.decode(errors="ignore") for p in f[b"path"]]
            paths.append("/".join(parts))
        return paths

    def _contains_track(self, filelist: List[str], track: str) -> bool:
        t = track.lower()
        return any(t in fname.lower() or fuzz.partial_ratio(t, fname.lower()) >= 80 for fname in filelist)

    def _search_sync(self, query: str, only_lossless: Optional[bool], track: Optional[str]) -> List[TorrentInfo]:
        cache_key = f"search:{query}:{only_lossless}:{track}"
        if raw := self.redis.get(cache_key):
            return [TorrentInfo(**d) for d in json.loads(raw)]

        self._ensure_login()

        search_url = f"{self.base_url}/forum/tracker.php"
        r0 = self.scraper.get(search_url, params={"nm": query}); r0.raise_for_status()
        doc0 = lxml.html.fromstring(r0.text)
        form = doc0.xpath("//form[@id='tr-form']")
        if not form:
            raise RuntimeError("Search form not found; login might have failed")
        form = form[0]

        action = form.get("action") or search_url
        if not action.startswith("http"):
            action = f"{self.base_url}/forum/{action.lstrip('/')}"
        post_data = {
            inp.get("name"): inp.get("value", "")
            for inp in form.xpath(".//input[@type='hidden']")
            if inp.get("name")
        }
        post_data.update({"nm": query, "f[]": "-1"})
        r1 = self.scraper.post(action, data=post_data); r1.raise_for_status()
        doc1 = lxml.html.fromstring(r1.text)

        text = doc1.text_content()
        total = int(_TOTAL_RE.search(text).group(1)) if _TOTAL_RE.search(text) else 0
        per_page = 50
        pages = ceil(total / per_page) if total else 1

        parsed: List[Tuple[TorrentInfo, int]] = []
        search_id = None
        for script in doc1.xpath("//script/text()"):
            if m := _PG_BASE_URL_RE.search(script):
                search_id = parse_qs(m.group(1)).get("search_id", [None])[0]
                break

        def parse_page(doc):
            for row in doc.xpath("//tr[@data-topic_id]"):
                href = row.xpath(".//a[contains(@href,'dl.php?t=')]/@href")[0]
                tid = int(parse_qs(urlparse(href).query)["t"][0])
                title = row.xpath(".//td[contains(@class,'t-title-col')]//a/text()")[0].strip()
                is_l = bool(_LOSSLESS_RE.search(title))
                is_y = bool(_LOSSY_RE.search(title))
                if only_lossless is True and (not is_l or is_y): continue
                if only_lossless is False and is_l: continue
                size = row.xpath(".//a[contains(@href,'dl.php?t=')]/text()")[0].strip()
                se = int(row.xpath(".//b[contains(@class,'seedmed')]/text()")[0] or 0)
                le = int(row.xpath(".//td[contains(@class,'leechmed')]/text()")[0] or 0)
                parsed.append((TorrentInfo(
                    title=title,
                    url=urljoin(self.base_url + "/forum/", href),
                    size=size,
                    seeders=se,
                    leechers=le
                ), tid))

        parse_page(doc1)
        if pages > 1 and search_id:
            for off in range(1, pages):
                resp = self.scraper.get(search_url, params={"search_id": search_id, "start": off * per_page})
                resp.raise_for_status()
                parse_page(lxml.html.fromstring(resp.text))

        if track:
            results = []
            for ti, tid in parsed:
                key2 = f"tracklist:{tid}"
                if raw2 := self.redis.get(key2):
                    fl = json.loads(raw2)
                else:
                    fl = self._parse_filelist(self._download_sync(tid))
                    self.redis.set(key2, json.dumps(fl), ex=3600)
                if self._contains_track(fl, track):
                    results.append(ti)
        else:
            results = [ti for ti, _ in parsed]

        to_cache = [{"title": r.title, "url": r.url, "size": r.size,
                     "seeders": r.seeders, "leechers": r.leechers}
                    for r in results]
        self.redis.set(cache_key, json.dumps(to_cache), ex=300)
        return results

    async def search(self, query: str, only_lossless: Optional[bool] = None, track: Optional[str] = None):
        import asyncio
        return await asyncio.to_thread(self._search_sync, query, only_lossless, track)

    def _download_sync(self, topic_id: int) -> bytes:
        self._ensure_login()
        dl_url = f"{self.base_url}/forum/dl.php?t={topic_id}"
        resp = self.scraper.get(dl_url, allow_redirects=True)
        resp.raise_for_status()
        return resp.content

    async def download(self, topic_id: int):
        import asyncio
        return await asyncio.to_thread(self._download_sync, topic_id)

    async def close(self):
        pass
