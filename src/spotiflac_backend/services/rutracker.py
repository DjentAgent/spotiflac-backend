# src/spotiflac_backend/services/rutracker.py

import logging
import re
from typing import List
from urllib.parse import urljoin

import cloudscraper
from bs4 import BeautifulSoup

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

log = logging.getLogger(__name__)


class RutrackerService:
    def __init__(self, base_url: str = None):
        self.base_url = (base_url or settings.rutracker_base).rstrip("/")
        self.scraper = cloudscraper.create_scraper(
            browser={"custom": "SpotiFlac/1.0"},
            delay=10,
        )

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
            raise RuntimeError("Login failed — bb_session или bb_sessionhash cookie не найдена")
        log.debug("Login successful, session_cookie=%s", session_cookie)

        # Follow redirect
        location = post.headers.get("Location")
        if location:
            if not location.startswith("http"):
                location = f"{self.base_url}/forum/{location.lstrip('/')}"
            log.debug("Following redirect to %s", location)
            final = self.scraper.get(location)
            final.raise_for_status()
            log.debug("Final GET after login status: %s", final.status_code)

    def _search_sync(self, query: str) -> List[TorrentInfo]:
        # 1) логинимся
        self._login_sync()

        # 2) GET формы поиска
        search_url = f"{self.base_url}/forum/tracker.php"
        log.debug("=== Search: GET %s?nm=%r ===", search_url, query)
        r = self.scraper.get(search_url, params={"nm": query})
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "lxml")
        form = soup.find("form", id="tr-form")
        if not form:
            raise RuntimeError("Advanced search form not found on tracker.php")

        # 3) Собираем POST‑данные
        action = form["action"]
        if not action.startswith("http"):
            action = f"{self.base_url}/forum/{action.lstrip('/')}"
        data = {
            inp["name"]: inp.get("value", "")
            for inp in form.find_all("input", {"type": "hidden"})
            if inp.has_attr("name")
        }
        data.update({
            "nm": query,
            "f[]": "-1",  # все разделы
        })

        # 4) POST
        log.debug("=== Search: POST %s ===", action)
        r = self.scraper.post(action, data=data)
        r.raise_for_status()

        # 5) Парсим результаты
        soup = BeautifulSoup(r.text, "lxml")
        rows = soup.select("tr[data-topic_id]")
        log.debug("Found %d result rows", len(rows))

        results: List[TorrentInfo] = []
        for idx, row in enumerate(rows, start=1):
            # Название раздачи
            title_a = row.select_one("td.t-title-col a.tLink")
            if not title_a:
                log.debug("Row #%d: no title link, skip", idx)
                continue
            title = title_a.text.strip()

            # Ссылка на torrent-файл и размер
            a_dl = row.find("a", href=re.compile(r"dl\.php\?t="))
            if not a_dl:
                log.debug("Row #%d: no dl.php link, skip", idx)
                continue
            torrent_href = a_dl["href"]
            torrent_url = urljoin(f"{self.base_url}/forum/", torrent_href)
            size = (
                a_dl.text
                .strip()
                .replace("\xa0", " ")
                .replace("↓", "")
                .strip()
            )

            # Сиды
            seed_tag = row.select_one("b.seedmed")
            seeders = int(seed_tag.text) if seed_tag and seed_tag.text.isdigit() else 0

            # Личеры
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

    async def close(self):
        pass
