# src/spotiflac_backend/services/rutracker.py

import logging
from typing import List
from bs4 import BeautifulSoup
import cloudscraper

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

        # 1) GET формы логина
        log.debug("=== Login: GET login page ===")
        resp = self.scraper.get(login_url)
        resp.raise_for_status()
        log.debug("Login GET status: %s", resp.status_code)

        soup = BeautifulSoup(resp.text, "lxml")
        form = soup.find("form", id="login-form-full")
        if not form:
            snippet = resp.text[:200].replace("\n", "")
            raise RuntimeError(f"Login form not found, snippet: {snippet!r}")

        # 2) Собираем action и hidden‑поля, жёстко на login_try=Y
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

        log.debug("=== Login: manual POST to %s ===", action)
        log.debug("POST data keys: %s", list(data.keys()))
        post_resp = self.scraper.post(
            action,
            data=data,
            headers=headers,
            allow_redirects=False,
        )
        log.debug("Login POST status: %s", post_resp.status_code)
        log.debug("Login POST headers: %s", post_resp.headers)

        if post_resp.status_code not in (302, 303):
            snippet = post_resp.text[:200].replace("\n", "")
            raise RuntimeError(f"Unexpected login response, snippet: {snippet!r}")

        # 5) Проверяем куку bb_session или bb_sessionhash
        cookies = self.scraper.cookies.get_dict()
        log.debug("Cookies after POST: %s", cookies)
        session_cookie = cookies.get("bb_sessionhash") or cookies.get("bb_session")
        if not session_cookie:
            raise RuntimeError("Login failed — bb_session или bb_sessionhash cookie не найдена")
        log.debug("Login successful, session_cookie=%s", session_cookie)

        # 6) Завершающий GET по редиректу
        location = post_resp.headers.get("Location")
        if location:
            if not location.startswith("http"):
                location = f"{self.base_url}/forum/{location.lstrip('/')}"
            log.debug("Following redirect to %s", location)
            final = self.scraper.get(location)
            final.raise_for_status()
            log.debug("Final page after login GET status: %s", final.status_code)

    def _search_sync(self, query: str) -> List[str]:
        # 1) логинимся на rutracker
        self._login_sync()

        # 2) GET формы расширенного поиска, чтобы собрать все скрытые поля
        search_url = f"{self.base_url}/forum/tracker.php"
        log.debug("=== Search: GET form %s?nm=%r ===", search_url, query)
        r = self.scraper.get(search_url, params={"nm": query})
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "lxml")
        form = soup.find("form", id="tr-form")
        if not form:
            raise RuntimeError("Advanced search form not found on tracker.php")

        # 3) Определяем URL для POST
        action = form["action"]
        if not action.startswith("http"):
            action = f"{self.base_url}/forum/{action.lstrip('/')}"

        # 4) Собираем все скрытые поля
        data = {
            inp["name"]: inp.get("value", "")
            for inp in form.find_all("input", {"type": "hidden"})
            if inp.has_attr("name")
        }
        # Подставляем наш поисковый запрос и устанавливаем f[] = -1 (все разделы)
        data["nm"] = query
        data["f[]"] = "-1"

        # 5) POST-им форму поиска
        log.debug("=== Search: POST %s ===", action)
        r = self.scraper.post(action, data=data)
        r.raise_for_status()

        # 6) Парсим результаты — строки с раздачами имеют атрибут data-topic_id
        soup = BeautifulSoup(r.text, "lxml")
        rows = soup.select("tr[data-topic_id]")
        log.debug("Found %d result rows", len(rows))

        for idx, row in enumerate(rows, 1):
            # получить весь текст в строке, очистив лишние пробелы
            text = row.get_text(separator=' ', strip=True)
            log.debug("Row %d text: %s", idx, text)
            print(f"{idx}: {text}\n")


        # 7) Собираем только названия
        titles: List[str] = []
        for row in rows:
            a = row.find("a", title=True)
            if a:
                titles.append(a["title"])
                log.debug("Parsed title: %s", a["title"])

        return titles

    async def search(self, query: str) -> List[TorrentInfo]:
        import asyncio
        return await asyncio.to_thread(self._search_sync, query)

    async def close(self):
        pass
