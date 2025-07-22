# src/spotiflac_backend/services/rutracker.py

import json
import logging
import re
from math import ceil
from typing import List, Optional, Tuple
from urllib.parse import urljoin, parse_qs, urlparse
from concurrent.futures import ThreadPoolExecutor

import bencodepy
import cloudscraper
import redis
import lxml.html
from lxml.etree import XPath
from rapidfuzz import fuzz

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

log = logging.getLogger(__name__)

# Регэкспы
_LOSSLESS_RE = re.compile(r"\b(flac|wavpack|wv|ape|alac|aiff|pcm|dts|mlp|tta|mqa|lossless)\b", re.IGNORECASE)
_LOSSY_RE    = re.compile(r"\b(mp3|aac|ogg|opus|lossy)\b", re.IGNORECASE)
_PG_BASE_URL_RE = re.compile(r"PG_BASE_URL\s*:\s*['\"][^?]+\?([^'\"]+)['\"]", re.IGNORECASE)
_TOTAL_RE = re.compile(r"Результатов поиска:\s*(\d+)", re.IGNORECASE)


def _parse_filelist(torrent_bytes: bytes) -> List[str]:
    meta = bencodepy.decode(torrent_bytes)
    info = meta[b"info"]
    if b"filename" in info:
        return [info[b"filename"].decode(errors="ignore")]
    paths = []
    for f in info.get(b"files", []):
        parts = [p.decode(errors="ignore") for p in f[b"path"]]
        paths.append("/".join(parts))
    return paths


def _contains_track(filelist: List[str], track: str) -> bool:
    t = track.lower()
    for fname in filelist:
        fl = fname.lower()
        if t in fl or fuzz.partial_ratio(t, fl) >= 80:
            return True
    return False


class RutrackerService:
    def __init__(self, base_url: str = None):
        self.base_url = (base_url or settings.rutracker_base).rstrip("/")
        self.scraper = cloudscraper.create_scraper(
            browser={"custom": "SpotiFlac/1.0"}, delay=10
        )
        self.redis = redis.Redis.from_url(settings.redis_url)
        self.cookie_ttl = getattr(settings, "rutracker_cookie_ttl", 24 * 3600)

        try:
            raw = self.redis.get("rutracker:cookiejar")
            if raw:
                jar = json.loads(raw)
                self.scraper.cookies.update(jar)
                log.debug("Loaded Rutracker cookies from Redis")
        except Exception:
            log.exception("Failed to load cookies from Redis, will re-login on demand")

    def _ensure_login(self):
        jar = self.scraper.cookies.get_dict()
        if jar.get("bb_sessionhash") or jar.get("bb_session"):
            return
        self._login_sync()

        try:
            new_jar = self.scraper.cookies.get_dict()
            self.redis.set("rutracker:cookiejar", json.dumps(new_jar), ex=self.cookie_ttl)
            log.debug("Saved new Rutracker cookies to Redis")
        except Exception:
            log.exception("Failed to save cookies to Redis")

    def _login_sync(self):
        login_url = f"{self.base_url}/forum/login.php"
        resp = self.scraper.get(login_url); resp.raise_for_status()
        doc = lxml.html.fromstring(resp.text)

        try:
            form = doc.get_element_by_id("login-form-full")
        except KeyError:
            raise RuntimeError("Login form not found on Rutracker (maybe blocked or changed layout)")

        action = form.action or "login.php"
        if not action.startswith("http"):
            action = f"{self.base_url}/forum/{action.lstrip('/')}?login_try=Y"

        data = {
            inp.get("name"): inp.get("value", "")
            for inp in form.xpath(".//input[@type='hidden']")
            if inp.get("name")
        }
        data.update({
            "login_username": settings.rutracker_login,
            "login_password": settings.rutracker_password,
            "login": form.xpath(".//input[@type='submit']")[0].get("value")
        })

        post = self.scraper.post(
            action, data=data,
            headers={"Referer": login_url, "User-Agent": "SpotiFlac/1.0"},
            allow_redirects=False
        )
        post.raise_for_status()

        cookies = self.scraper.cookies.get_dict()
        if not (cookies.get("bb_sessionhash") or cookies.get("bb_session")):
            raise RuntimeError("Login failed, session cookie not found")

        loc = post.headers.get("Location")
        if loc:
            if not loc.startswith("http"):
                loc = f"{self.base_url}/forum/{loc.lstrip('/')}"
            self.scraper.get(loc).raise_for_status()

    def _search_sync(self, query: str, only_lossless: Optional[bool] = None, track: Optional[str] = None) -> List[TorrentInfo]:
        cache_key = f"search:{query}:{only_lossless}:{track}"
        if (raw := self.redis.get(cache_key)):
            return [TorrentInfo(**d) for d in json.loads(raw)]

        self._ensure_login()

        search_url = f"{self.base_url}/forum/tracker.php"
        r0 = self.scraper.get(search_url, params={"nm": query}); r0.raise_for_status()
        doc0 = lxml.html.fromstring(r0.text)

        try:
            form = doc0.get_element_by_id("tr-form")
        except KeyError:
            raise RuntimeError("'tr-form' not found on search page (maybe layout changed or cookies blocked)")

        action = form.action or search_url
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

        total = int(_TOTAL_RE.search(doc1.text_content()).group(1)) if _TOTAL_RE.search(doc1.text_content()) else 0
        per_page = 50
        pages = ceil(total / per_page) if total else 1

        search_id = None
        for script in doc1.xpath("//script/text()"):
            if m := _PG_BASE_URL_RE.search(script):
                qs = parse_qs(m.group(1))
                search_id = qs.get("search_id", [None])[0]
                break

        parsed: List[Tuple[TorrentInfo, int]] = []

        def _parse_page(doc, offset: int):
            rows = doc.xpath("//tr[@data-topic_id]")
            for row in rows:
                href = row.xpath(".//a[contains(@href,'dl.php?t=')]/@href")[0]
                tid = int(parse_qs(urlparse(href).query)["t"][0])

                fx = row.xpath(".//td[contains(@class,'f-name-col')]//a/text()")
                forum_txt = fx[0].strip() if fx else ""
                title = row.xpath(".//td[contains(@class,'t-title-col')]//a/text()")[0].strip()

                combined = f"{forum_txt} {title}"
                is_l = bool(_LOSSLESS_RE.search(combined))
                is_y = bool(_LOSSY_RE.search(combined))
                if only_lossless is True and (not is_l or is_y): continue
                if only_lossless is False and is_l: continue

                size = row.xpath(".//a[contains(@href,'dl.php?t=')]/text()")[0] \
                    .strip().replace("\xa0", " ").replace("↓", "").strip()
                se = int(row.xpath(".//b[contains(@class,'seedmed')]/text()")[0] or 0)
                le = int(row.xpath(".//td[contains(@class,'leechmed')]/text()")[0] or 0)

                parsed.append((
                    TorrentInfo(title=title, url=urljoin(f"{self.base_url}/forum/", href), size=size, seeders=se, leechers=le),
                    tid
                ))

        _parse_page(doc1, 0)

        if pages > 1 and search_id:
            offsets = [i * per_page for i in range(1, pages)]
            def fetch_parse(off: int):
                resp = self.scraper.get(search_url, params={"search_id": search_id, "start": off})
                resp.raise_for_status()
                d = lxml.html.fromstring(resp.text)
                _parse_page(d, off)

            with ThreadPoolExecutor(max_workers=min(4, len(offsets))) as ex:
                ex.map(fetch_parse, offsets)

        if track:
            def process_entry(entry: Tuple[TorrentInfo, int]) -> Optional[TorrentInfo]:
                ti, tid = entry
                key = f"tracklist:{tid}"
                if (raw_list := self.redis.get(key)):
                    fl = json.loads(raw_list)
                else:
                    data = self._download_sync(tid)
                    fl = _parse_filelist(data)
                    self.redis.set(key, json.dumps(fl), ex=3600)
                return ti if _contains_track(fl, track) else None

            filtered: List[TorrentInfo] = []
            with ThreadPoolExecutor(max_workers=5) as ex:
                for result in ex.map(process_entry, parsed):
                    if result:
                        filtered.append(result)
            results = filtered
        else:
            results = [ti for ti, _ in parsed]

        to_cache = [
            {"title": r.title, "url": r.url, "size": r.size,
             "seeders": r.seeders, "leechers": r.leechers}
            for r in results
        ]
        self.redis.set(cache_key, json.dumps(to_cache), ex=300)
        return results

    async def search(self, query: str, only_lossless: Optional[bool] = None, track: Optional[str] = None) -> List[TorrentInfo]:
        import asyncio
        return await asyncio.to_thread(self._search_sync, query, only_lossless, track)

    def _download_sync(self, topic_id: int) -> bytes:
        self._ensure_login()
        dl_url = f"{self.base_url}/forum/dl.php?t={topic_id}"
        resp = self.scraper.get(dl_url, allow_redirects=True)
        resp.raise_for_status()
        return resp.content

    async def download(self, topic_id: int) -> bytes:
        import asyncio
        return await asyncio.to_thread(self._download_sync, topic_id)

    async def close(self):
        pass