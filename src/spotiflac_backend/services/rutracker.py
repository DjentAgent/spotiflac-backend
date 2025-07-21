# src/spotiflac_backend/services/rutracker.py

import json
import logging
import re
from math import ceil
from typing import List, Optional, Tuple
from urllib.parse import urljoin, parse_qs, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import bencodepy
import cloudscraper
import redis
from bs4 import BeautifulSoup

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

log = logging.getLogger(__name__)

# Регэкспы для фильтра lossless / lossy и поиска search_id/total
_LOSSLESS_RE     = re.compile(r"\b(flac|wavpack|ape|alac)\b", re.IGNORECASE)
_LOSSY_RE        = re.compile(r"\b(mp3|aac|ogg|opus)\b", re.IGNORECASE)
_PG_BASE_URL_RE  = re.compile(r"PG_BASE_URL\s*:\s*['\"][^?]+\?([^'\"]+)['\"]", re.IGNORECASE)
_TOTAL_RE        = re.compile(r"Результатов поиска:\s*(\d+)", re.IGNORECASE)


def _parse_filelist(torrent_bytes: bytes) -> List[str]:
    """Распаковывает .torrent и возвращает список путей файлов внутри раздачи."""
    meta = bencodepy.decode(torrent_bytes)
    info = meta[b"info"]
    # single-file
    if b"filename" in info:
        return [info[b"filename"].decode(errors="ignore")]
    # multi-file
    files = info.get(b"files", [])
    paths: List[str] = []
    for f in files:
        parts = [p.decode(errors="ignore") for p in f[b"path"]]
        paths.append("/".join(parts))
    return paths


def _contains_track(filelist: List[str], track: str) -> bool:
    """Проверяет, встречается ли трек track в любом из путей filelist."""
    t = track.lower()
    for fname in filelist:
        fl = fname.lower()
        if t in fl:
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

        # Попытка восстановить куки из Redis
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
        if jar.get("bb_session") or jar.get("bb_sessionhash"):
            return

        self._login_sync()
        try:
            new_jar = self.scraper.cookies.get_dict()
            self.redis.set("rutracker:cookiejar", json.dumps(new_jar), ex=self.cookie_ttl)
            log.debug("Saved new Rutracker cookies to Redis (ttl=%d)", self.cookie_ttl)
        except Exception:
            log.exception("Failed to save cookies to Redis")

    def _login_sync(self):
        login_url = f"{self.base_url}/forum/login.php"
        log.debug("=== Login: GET %s ===", login_url)
        resp = self.scraper.get(login_url); resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        form = soup.find("form", id="login-form-full")
        if not form:
            snippet = resp.text[:200].replace("\n", "")
            raise RuntimeError(f"Login form not found, snippet: {snippet!r}")

        action = form.get("action", "login.php")
        if not action.startswith("http"):
            action = f"{self.base_url}/forum/{action.lstrip('/')}?login_try=Y"

        data = {
            inp["name"]: inp.get("value", "")
            for inp in form.find_all("input", {"type": "hidden"})
            if inp.has_attr("name")
        }
        data.update({
            "login_username": settings.rutracker_login,
            "login_password": settings.rutracker_password,
            "login": form.find("input", {"type": "submit"})["value"],
        })
        headers = {"Referer": login_url, "User-Agent": "SpotiFlac/1.0"}

        log.debug("=== Login: POST %s ===", action)
        post = self.scraper.post(action, data=data, headers=headers, allow_redirects=False)
        post.raise_for_status()

        cookies = self.scraper.cookies.get_dict()
        if not (cookies.get("bb_sessionhash") or cookies.get("bb_session")):
            raise RuntimeError("Login failed — bb_session или bb_sessionhash cookie не найдена")
        log.debug("Login successful, session_cookie=%s",
                  cookies.get("bb_sessionhash") or cookies.get("bb_session"))

        location = post.headers.get("Location")
        if location:
            if not location.startswith("http"):
                location = f"{self.base_url}/forum/{location.lstrip('/')}"
            log.debug("Following redirect to %s", location)
            final = self.scraper.get(location); final.raise_for_status()
            log.debug("Final GET after login status: %s", final.status_code)

    def _download_sync(self, topic_id: int) -> bytes:
        self._ensure_login()
        dl_url = f"{self.base_url}/forum/dl.php?t={topic_id}"
        log.debug("Downloading torrent %s", dl_url)
        resp = self.scraper.get(dl_url, allow_redirects=True)
        resp.raise_for_status()
        return resp.content

    def _search_sync(
        self,
        query: str,
        only_lossless: Optional[bool] = None,
        track: Optional[str] = None
    ) -> List[TorrentInfo]:
        # 1) кеш поиска
        cache_key = f"search:{query}:{only_lossless}:{track}"
        raw = self.redis.get(cache_key)
        if raw:
            log.debug("Returning cached search results for %s", cache_key)
            data = json.loads(raw)
            return [TorrentInfo(**d) for d in data]

        self._ensure_login()
        search_url = f"{self.base_url}/forum/tracker.php"

        # 2) GET+POST первой страницы
        log.debug("=== Search: GET %s?nm=%r ===", search_url, query)
        r0 = self.scraper.get(search_url, params={"nm": query}); r0.raise_for_status()
        soup0 = BeautifulSoup(r0.text, "lxml")
        form = soup0.find("form", id="tr-form")
        if not form:
            raise RuntimeError("Advanced search form not found")

        action = form["action"]
        if not action.startswith("http"):
            action = f"{self.base_url}/forum/{action.lstrip('/')}"

        post_data = {
            inp["name"]: inp.get("value", "")
            for inp in form.find_all("input", {"type": "hidden"})
            if inp.has_attr("name")
        }
        post_data.update({"nm": query, "f[]": "-1"})
        log.debug("=== Search: POST %s ===", action)
        r1 = self.scraper.post(action, data=post_data); r1.raise_for_status()
        soup1 = BeautifulSoup(r1.text, "lxml")

        # 3) total + пагинация
        txt = soup1.get_text()
        m_tot = _TOTAL_RE.search(txt)
        total = int(m_tot.group(1)) if m_tot else 0
        per_page = 50
        pages = ceil(total / per_page) if total else 1

        search_id = None
        for script in soup1.find_all("script"):
            s = script.string or ""
            m = _PG_BASE_URL_RE.search(s)
            if m:
                qs = parse_qs(m.group(1))
                search_id = qs.get("search_id", [None])[0]
                log.debug("Found search_id=%s", search_id)
                break

        # 4) парсинг всех страниц (собираем пары (TorrentInfo, topic_id))
        parsed: List[Tuple[TorrentInfo, int]] = []

        def _parse_page(soup_page, offset: int):
            rows = soup_page.select("tr[data-topic_id]")
            log.debug("Parsing page offset=%d, rows=%d", offset, len(rows))
            for row in rows:
                href = row.find("a", href=re.compile(r"dl\.php\?t="))["href"]
                tid = int(parse_qs(urlparse(href).query)["t"][0])
                title = row.select_one("td.t-title-col a.tLink").text.strip()

                is_l = bool(_LOSSLESS_RE.search(title))
                is_y = bool(_LOSSY_RE.search(title))
                if only_lossless is True and (not is_l or is_y):
                    continue
                if only_lossless is False and is_l:
                    continue

                ti = TorrentInfo(
                    title=title,
                    url=urljoin(f"{self.base_url}/forum/", href),
                    size=row.find("a", href=re.compile(r"dl\.php\?t=")).text
                        .strip().replace("\xa0", " ").replace("↓", "").strip(),
                    seeders=int(row.select_one("b.seedmed").text or 0),
                    leechers=int(row.select_one("td.leechmed").text or 0),
                )
                parsed.append((ti, tid))

        # первая страница
        _parse_page(soup1, 0)

        # остальные страницы
        if pages > 1 and search_id:
            offsets = [i * per_page for i in range(1, pages)]
            for off in offsets:
                resp = self.scraper.get(search_url, params={"search_id": search_id, "start": off})
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "lxml")
                _parse_page(soup, off)

        # 5) если не нужно искать внутри .torrent — возвращаем сразу
        if not track:
            results = [ti for ti, _ in parsed]
        else:
            # 6) параллельный fetch+parse .torrent
            filtered: List[TorrentInfo] = []

            def fetch_and_parse(tid: int) -> Tuple[int, List[str]]:
                data = self._download_sync(tid)
                return tid, _parse_filelist(data)

            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_ti = {
                    executor.submit(fetch_and_parse, tid): ti
                    for ti, tid in parsed
                }
                for fut in as_completed(future_to_ti):
                    ti = future_to_ti[fut]
                    try:
                        tid, filelist = fut.result()
                    except Exception as e:
                        log.error("Error fetching/parsing torrent %s: %s", ti.url, e)
                        continue
                    if _contains_track(filelist, track):
                        filtered.append(ti)

            results = filtered

        # 7) кешируем выдачу на 5 минут
        to_cache = [
            {"title": r.title, "url": r.url, "size": r.size,
             "seeders": r.seeders, "leechers": r.leechers}
            for r in results
        ]
        self.redis.set(cache_key, json.dumps(to_cache), ex=300)

        return results

    async def search(
        self,
        query: str,
        only_lossless: Optional[bool] = None,
        track: Optional[str] = None
    ) -> List[TorrentInfo]:
        import asyncio
        return await asyncio.to_thread(self._search_sync, query, only_lossless, track)

    async def download(self, topic_id: int) -> bytes:
        import asyncio
        return await asyncio.to_thread(self._download_sync, topic_id)

    async def close(self):
        # Ничего не нужно чистить
        pass