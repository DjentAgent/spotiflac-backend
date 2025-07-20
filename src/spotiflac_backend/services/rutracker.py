# src/spotiflac_backend/services/rutracker.py

import json
import logging
import re
from typing import List, Optional
from urllib.parse import urljoin, parse_qs

import cloudscraper
import redis
from bs4 import BeautifulSoup

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

log = logging.getLogger(__name__)

# Регэкспы для фильтра lossless / lossy
_LOSSLESS_RE = re.compile(r"\b(flac|wavpack|ape|alac)\b", re.IGNORECASE)
_LOSSY_RE    = re.compile(r"\b(mp3|aac|ogg|opus)\b", re.IGNORECASE)


class RutrackerService:
    def __init__(self, base_url: str = None):
        self.base_url = (base_url or settings.rutracker_base).rstrip("/")
        self.scraper = cloudscraper.create_scraper(
            browser={"custom": "SpotiFlac/1.0"},
            delay=10,
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

        # Сохраняем новую куку‑банку
        try:
            new_jar = self.scraper.cookies.get_dict()
            self.redis.set(
                "rutracker:cookiejar",
                json.dumps(new_jar),
                ex=self.cookie_ttl
            )
            log.debug("Saved new Rutracker cookies to Redis (ttl=%d)", self.cookie_ttl)
        except Exception:
            log.exception("Failed to save cookies to Redis")

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
        if not (cookies.get("bb_sessionhash") or cookies.get("bb_session")):
            raise RuntimeError("Login failed — bb_session или bb_sessionhash cookie не найдена")
        log.debug("Login successful, session_cookie=%s", cookies.get("bb_sessionhash") or cookies.get("bb_session"))

        # follow redirect
        location = post.headers.get("Location")
        if location:
            if not location.startswith("http"):
                location = f"{self.base_url}/forum/{location.lstrip('/')}"
            log.debug("Following redirect to %s", location)
            final = self.scraper.get(location)
            final.raise_for_status()
            log.debug("Final GET after login status: %s", final.status_code)

    def _search_sync(
        self,
        query: str,
        only_lossless: Optional[bool] = None
    ) -> List[TorrentInfo]:
        self._ensure_login()

        search_url = f"{self.base_url}/forum/tracker.php"
        # 1) Получаем саму страницу с формой (GET)
        log.debug("=== Search: GET %s?nm=%r ===", search_url, query)
        r = self.scraper.get(search_url, params={"nm": query})
        r.raise_for_status()

        # 2) Собираем POST‑данные из формы
        soup = BeautifulSoup(r.text, "lxml")
        form = soup.find("form", id="tr-form")
        if not form:
            raise RuntimeError("Advanced search form not found on tracker.php")

        action = form["action"]
        if not action.startswith("http"):
            action = f"{self.base_url}/forum/{action.lstrip('/')}"
        post_data = {
            inp["name"]: inp.get("value", "")
            for inp in form.find_all("input", {"type": "hidden"})
            if inp.has_attr("name")
        }
        post_data.update({"nm": query, "f[]": "-1"})

        # 3) Первый POST – выдаёт первую страницу результатов
        log.debug("=== Search: POST %s ===", action)
        r = self.scraper.post(action, data=post_data)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # 4) Выдернем из JS search_id для последующих GET-страниц
        search_id = None
        for script in soup.find_all("script"):
            txt = script.string or ""
            m = re.search(r"PG_BASE_URL\s*:\s*'[^?]+\?([^']+)'", txt)
            if m:
                qs = parse_qs(m.group(1))
                search_id = qs.get("search_id", [None])[0]
                break
        if not search_id:
            raise RuntimeError("Не удалось найти search_id для пагинации")

        results: List[TorrentInfo] = []
        page = 0
        per_page = 50

        while True:
            # 5) Парсим все строки на этой странице
            rows = soup.select("tr[data-topic_id]")
            log.debug("Page %d: found %d rows", page + 1, len(rows))
            if not rows:
                break

            for idx, row in enumerate(rows, start=1 + page * per_page):
                title_a = row.select_one("td.t-title-col a.tLink")
                if not title_a:
                    continue
                title = title_a.text.strip()

                is_lossless = bool(_LOSSLESS_RE.search(title))
                is_lossy    = bool(_LOSSY_RE.search(title))

                # применяем фильтр
                if only_lossless is True and (not is_lossless or is_lossy):
                    continue
                if only_lossless is False and is_lossless:
                    continue

                a_dl = row.find("a", href=re.compile(r"dl\.php\?t="))
                if not a_dl:
                    continue
                torrent_url = urljoin(f"{self.base_url}/forum/", a_dl["href"])
                size = a_dl.text.strip().replace("\xa0", " ").replace("↓", "").strip()

                seed_tag = row.select_one("b.seedmed")
                seeders = int(seed_tag.text) if seed_tag and seed_tag.text.isdigit() else 0

                leech_tag = row.select_one("td.leechmed")
                leechers = int(leech_tag.text) if leech_tag and leech_tag.text.isdigit() else 0

                results.append(TorrentInfo(
                    title=title,
                    url=torrent_url,
                    size=size,
                    seeders=seeders,
                    leechers=leechers,
                ))

            # 6) Если строк меньше чем на страницу — это была последняя
            if len(rows) < per_page:
                break

            # 7) Идём на следующую страницу
            page += 1
            offset = page * per_page
            log.debug("Fetching page %d (start=%d)", page + 1, offset)
            r = self.scraper.get(
                search_url,
                params={"search_id": search_id, "start": offset}
            )
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")

        return results

    async def search(
        self,
        query: str,
        only_lossless: Optional[bool] = None
    ) -> List[TorrentInfo]:
        import asyncio
        return await asyncio.to_thread(self._search_sync, query, only_lossless)

    def _download_sync(self, topic_id: int) -> bytes:
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
        # ничего не нужно закрывать
        pass