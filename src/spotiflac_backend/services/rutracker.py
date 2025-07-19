# src/spotiflac_backend/services/rutracker.py

import json
import logging
import re
from typing import List
from urllib.parse import urljoin

import cloudscraper
from bs4 import BeautifulSoup

import redis
from redis.exceptions import RedisError

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

log = logging.getLogger(__name__)


class RutrackerService:
    def __init__(self, base_url: str = None):
        self.base_url = (base_url or settings.rutracker_base).rstrip("/")
        # cloudscraper для обхода Cloudflare
        self.scraper = cloudscraper.create_scraper(
            browser={"custom": "SpotiFlac/1.0"},
            delay=10,
        )
        # TTL хранения cookies в секундах
        self.cookie_ttl = getattr(settings, "rutracker_cookie_ttl", 24 * 3600)

        # Попытка инициализировать Redis‑клиент и загрузить cookies
        self.redis = None
        try:
            self.redis = redis.Redis.from_url(settings.redis_url, socket_timeout=1)
            raw = self.redis.get("rutracker:cookiejar")
            if raw:
                jar = json.loads(raw)
                self.scraper.cookies.update(jar)
                log.debug("Loaded Rutracker cookies from Redis")
        except RedisError:
            log.warning("Redis unavailable, proceeding without cookie cache", exc_info=True)

    def _ensure_login(self):
        """
        Убедиться, что в scraper есть действующая sesssion‑cookie.
        Если нет — выполнить _login_sync() и закешировать cookies.
        """
        jar = self.scraper.cookies.get_dict()
        if jar.get("bb_session") or jar.get("bb_sessionhash"):
            return  # уже залогинены

        # нет действующей сессии — логинимся
        self._login_sync()

        # после логина пытаемся сохранить cookies
        if self.redis:
            try:
                new_jar = self.scraper.cookies.get_dict()
                self.redis.set(
                    "rutracker:cookiejar",
                    json.dumps(new_jar),
                    ex=self.cookie_ttl
                )
                log.debug("Saved Rutracker cookies to Redis (ttl=%d)", self.cookie_ttl)
            except RedisError:
                log.warning("Failed to save cookies to Redis", exc_info=True)

    def _login_sync(self):
        login_url = f"{self.base_url}/forum/login.php"
        log.debug("=== Login: GET %s ===", login_url)
        resp = self.scraper.get(login_url)
        resp.raise_for_status()

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

        headers = {
            "Referer": login_url,
            "User-Agent": "SpotiFlac/1.0",
        }

        log.debug("=== Login: POST %s ===", action)
        post = self.scraper.post(
            action,
            data=data,
            headers=headers,
            allow_redirects=False,
        )
        post.raise_for_status()

        cookies = self.scraper.cookies.get_dict()
        session_cookie = cookies.get("bb_sessionhash") or cookies.get("bb_session")
        if not session_cookie:
            raise RuntimeError("Login failed — bb_session or bb_sessionhash cookie not found")
        log.debug("Login successful, session_cookie=%s", session_cookie)

        # Follow redirect if present
        location = post.headers.get("Location")
        if location:
            if not location.startswith("http"):
                location = f"{self.base_url}/forum/{location.lstrip('/')}"
            log.debug("Following redirect to %s", location)
            final = self.scraper.get(location)
            final.raise_for_status()
            log.debug("Final GET after login status: %s", final.status_code)

    def _search_sync(self, query: str) -> List[TorrentInfo]:
        # гарантируем, что залогинены
        self._ensure_login()

        # 1) GET форму поиска
        search_url = f"{self.base_url}/forum/tracker.php"
        log.debug("=== Search: GET %s?nm=%r ===", search_url, query)
        r = self.scraper.get(search_url, params={"nm": query})
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "lxml")
        form = soup.find("form", id="tr-form")
        if not form:
            raise RuntimeError("Advanced search form not found on tracker.php")

        # 2) Собираем POST‑данные
        action = form["action"]
        if not action.startswith("http"):
            action = f"{self.base_url}/forum/{action.lstrip('/')}"
        data = {
            inp["name"]: inp.get("value", "")
            for inp in form.find_all("input", {"type": "hidden"})
            if inp.has_attr("name")
        }
        data.update({"nm": query, "f[]": "-1"})

        # 3) POST запрос
        log.debug("=== Search: POST %s ===", action)
        r = self.scraper.post(action, data=data)
        r.raise_for_status()

        # 4) Парсим результаты
        soup = BeautifulSoup(r.text, "lxml")
        rows = soup.select("tr[data-topic_id]")
        log.debug("Found %d result rows", len(rows))

        results: List[TorrentInfo] = []
        for idx, row in enumerate(rows, start=1):
            title_a = row.select_one("td.t-title-col a.tLink")
            if not title_a:
                log.debug("Row #%d: no title link, skip", idx)
                continue
            title = title_a.text.strip()

            a_dl = row.find("a", href=re.compile(r"dl\.php\?t="))
            if not a_dl:
                log.debug("Row #%d: no dl.php link, skip", idx)
                continue
            torrent_url = urljoin(f"{self.base_url}/forum/", a_dl["href"])
            size = a_dl.text.strip().replace("\xa0", " ").replace("↓", "").strip()

            seed_tag = row.select_one("b.seedmed")
            seeders = int(seed_tag.text) if seed_tag and seed_tag.text.isdigit() else 0

            leech_tag = row.select_one("td.leechmed")
            leechers = int(leech_tag.text) if leech_tag and leech_tag.text.isdigit() else 0

            log.debug(
                "Row #%d: %s -> %s (%s, %d seed, %d leech)",
                idx, title, torrent_url, size, seeders, leechers
            )

            results.append(TorrentInfo(
                title=title,
                url=torrent_url,
                size=size,
                seeders=seeders,
                leechers=leechers,
            ))

        return results

    async def search(self, query: str) -> List[TorrentInfo]:
        import asyncio
        try:
            return await asyncio.to_thread(self._search_sync, query)
        except Exception:
            log.exception("RutrackerService.search() failed")
            return []

    def _download_sync(self, topic_id: int) -> bytes:
        # убедимся, что залогинены
        self._ensure_login()

        dl_url = f"{self.base_url}/forum/dl.php?t={topic_id}"
        log.debug("Downloading torrent %s", dl_url)
        resp = self.scraper.get(dl_url, allow_redirects=True)
        resp.raise_for_status()
        return resp.content

    async def download(self, topic_id: int) -> bytes:
        import asyncio
        return await asyncio.to_thread(self._download_sync, topic_id)

    async def close(self):
        # нет явных ресурсов для закрытия
        pass
