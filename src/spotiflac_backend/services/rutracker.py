import json
import logging
import re
from math import ceil
from typing import List, Optional, Tuple
from urllib.parse import urljoin, parse_qs, urlparse

import bencodepy
import cloudscraper
import redis
import lxml.html
from concurrent.futures import ThreadPoolExecutor
from rapidfuzz import fuzz

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

log = logging.getLogger(__name__)

# Регулярные выражения для фильтрации аудиоформатов и подсчета результатов
_LOSSLESS_RE = re.compile(r"\b(flac|wavpack|wv|ape|alac|aiff|pcm|dts|mlp|tta|mqa|lossless)\b", re.IGNORECASE)
_LOSSY_RE = re.compile(r"\b(mp3|aac|ogg|opus|lossy)\b", re.IGNORECASE)
_TOTAL_RE = re.compile(r"Результатов поиска:\s*(\d+)", re.IGNORECASE)
_PG_BASE_URL_RE = re.compile(r"PG_BASE_URL\s*:\s*['\"][^?]+\?([^'\"]+)['\"]", re.IGNORECASE)


def _parse_filelist(torrent_bytes: bytes) -> List[str]:
    meta = bencodepy.decode(torrent_bytes)
    info = meta[b"info"]
    if b"filename" in info:
        return [info[b"filename"].decode(errors="ignore")]
    return ["/".join(p.decode(errors="ignore") for p in f[b"path"]) for f in info.get(b"files", [])]


def _contains_track(filelist: List[str], track: str) -> bool:
    t = track.lower()
    return any(t in f.lower() or fuzz.partial_ratio(t, f.lower()) >= 80 for f in filelist)


class RutrackerService:
    def __init__(self, base_url: str = None):
        self.base_url = (base_url or settings.rutracker_base).rstrip("/")
        # Используем js2py-интерпретатор для обхода Cloudflare v3-challenge
        self.scraper = cloudscraper.create_scraper(
            interpreter='js2py',
            delay=10,
            browser={"custom": "SpotiFlac/1.0"},
            debug=False
        )
        self.redis = redis.Redis.from_url(settings.redis_url)
        self.cookie_ttl = getattr(settings, "rutracker_cookie_ttl", 86400)

        # Попытка загрузить ранее сохраненные cookies
        try:
            raw = self.redis.get("rutracker:cookiejar")
            if raw:
                self.scraper.cookies.update(json.loads(raw))
                log.debug("Loaded Rutracker cookies from Redis")
        except Exception:
            log.exception("Failed to load cookies from Redis")

    def _ensure_login(self):
        """Проверяет валидность сессии; при редиректе — реаутентифицируется."""
        r = self.scraper.get(f"{self.base_url}/forum/tracker.php", allow_redirects=False)
        if r.is_redirect and "login.php" in r.headers.get("Location", ""):
            log.info("Session invalid or expired, re-authenticating...")
            self.redis.delete("rutracker:cookiejar")
            self._login_sync()
        else:
            log.debug("Rutracker session valid")

    def _login_sync(self):
        login_url = f"{self.base_url}/forum/login.php"
        r = self.scraper.get(login_url)
        r.raise_for_status()
        doc = lxml.html.fromstring(r.text)

        # Находим форму логина по action, содержащему login.php
        form = None
        for f in doc.xpath("//form"):
            if "login.php" in f.get("action", ""):
                form = f
                break
        if form is None:
            raise RuntimeError("Login form not found — возможно изменился HTML-шаблон")

        # Готовим URL для POST
        raw_action = form.get("action", "")
        post_url = raw_action if raw_action.startswith("http") else urljoin(login_url, raw_action)

        # Сбор всех полей формы
        data = {inp.get("name"): inp.get("value", "") for inp in form.xpath(".//input[@name]")}
        # Вставляем логин и пароль
        data["login_username"] = settings.rutracker_login
        data["login_password"] = settings.rutracker_password

        # Выполняем POST-запрос и следим за редиректами
        post = self.scraper.post(post_url, data=data, headers={"Referer": login_url}, allow_redirects=True)
        post.raise_for_status()

        # Проверяем наличие сессионных cookie
        cookies = self.scraper.cookies.get_dict()
        if not (cookies.get("bb_session") or cookies.get("bb_sessionhash")):
            raise RuntimeError(
                "Login failed: session cookies not found — проверьте credentials или новый HTML формы"
            )

        # Сохраняем cookies в Redis
        try:
            self.redis.set("rutracker:cookiejar", json.dumps(cookies), ex=self.cookie_ttl)
            log.debug("Saved new Rutracker cookies to Redis")
        except Exception:
            log.exception("Failed to save cookies to Redis")

    def _search_sync(self, query: str, only_lossless: Optional[bool], track: Optional[str]) -> List[TorrentInfo]:
        cache_key = f"search:{query}:{only_lossless}:{track}"
        if (raw := self.redis.get(cache_key)):
            return [TorrentInfo(**r) for r in json.loads(raw)]

        self._ensure_login()

        # Первая страница поиска
        r0 = self.scraper.get(f"{self.base_url}/forum/tracker.php", params={"nm": query})
        r0.raise_for_status()
        doc0 = lxml.html.fromstring(r0.text)

        try:
            form = doc0.get_element_by_id("tr-form")
        except KeyError:
            raise RuntimeError("'tr-form' not found — возможно не залогинены")

        action = form.action or f"{self.base_url}/forum/tracker.php"
        if not action.startswith("http"):
            action = f"{self.base_url}/forum/{action.lstrip('/')}"

        post_data = {i.name: i.value for i in form.xpath(".//input[@type='hidden']") if i.name}
        post_data.update({"nm": query, "f[]": "-1"})

        r1 = self.scraper.post(action, data=post_data)
        r1.raise_for_status()
        doc1 = lxml.html.fromstring(r1.text)

        total = int(_TOTAL_RE.search(doc1.text_content()).group(1)) if _TOTAL_RE.search(doc1.text_content()) else 0
        per_page = 50
        pages = ceil(total / per_page) or 1

        # Извлекаем search_id для пагинации
        search_id = None
        for script in doc1.xpath("//script/text()"):
            if m := _PG_BASE_URL_RE.search(script):
                search_id = parse_qs(m.group(1)).get("search_id", [None])[0]
                break

        parsed: List[Tuple[TorrentInfo, int]] = []

        def _parse_page(doc):
            for row in doc.xpath("//tr[@data-topic_id]"):
                try:
                    href = row.xpath(".//a[contains(@href,'dl.php?t=')]/@href")[0]
                    tid = int(parse_qs(urlparse(href).query)["t"][0])
                    forum_txt = " ".join(row.xpath(".//td[contains(@class,'f-name-col')]//a/text()")).strip()
                    title = row.xpath(".//td[contains(@class,'t-title-col')]//a/text()")[0].strip()
                    combined = f"{forum_txt} {title}"
                    is_l = bool(_LOSSLESS_RE.search(combined))
                    is_y = bool(_LOSSY_RE.search(combined))
                    if only_lossless is True and (not is_l or is_y):
                        continue
                    if only_lossless is False and is_l:
                        continue
                    size = row.xpath(".//a[contains(@href,'dl.php?t=')]/text()")[0].replace("\xa0", " ").strip("↓ ").strip()
                    se = int(row.xpath(".//b[contains(@class,'seedmed')]/text()")[0] or 0)
                    le = int(row.xpath(".//td[contains(@class,'leechmed')]/text()")[0] or 0)
                    parsed.append((
                        TorrentInfo(
                            title=title,
                            url=urljoin(self.base_url + "/forum/", href),
                            size=size,
                            seeders=se,
                            leechers=le
                        ), tid
                    ))
                except Exception as e:
                    log.warning("Failed to parse row: %s", e)

        # Парсим первую страницу
        _parse_page(doc1)

        # Параллельная загрузка остальных страниц
        if pages > 1 and search_id:
            offsets = [i * per_page for i in range(1, pages)]
            with ThreadPoolExecutor(max_workers=min(4, len(offsets))) as ex:
                ex.map(lambda off: _parse_page(lxml.html.fromstring(
                    self.scraper.get(
                        f"{self.base_url}/forum/tracker.php",
                        params={"search_id": search_id, "start": off}
                    ).text
                )), offsets)

        # Дополнительная фильтрация по треку внутри .torrent
        if track:
            def check_track(item: Tuple[TorrentInfo, int]) -> Optional[TorrentInfo]:
                ti, tid = item
                key = f"tracklist:{tid}"
                if (raw := self.redis.get(key)):
                    fl = json.loads(raw)
                else:
                    try:
                        data = self._download_sync(tid)
                        fl = _parse_filelist(data)
                        self.redis.set(key, json.dumps(fl), ex=3600)
                    except Exception as e:
                        log.warning("Failed to download/parse tracklist for %s: %s", tid, e)
                        return None
                return ti if _contains_track(fl, track) else None

            with ThreadPoolExecutor(max_workers=5) as ex:
                filtered = [r for r in ex.map(check_track, parsed) if r]
            return filtered

        # Кэшируем результаты поиска
        results = [ti for ti, _ in parsed]
        self.redis.set(cache_key, json.dumps([r.model_dump() for r in results]), ex=300)
        return results

    async def search(self, query: str, only_lossless: Optional[bool] = None, track: Optional[str] = None) -> List[TorrentInfo]:
        import asyncio
        return await asyncio.to_thread(self._search_sync, query, only_lossless, track)

    def _download_sync(self, topic_id: int) -> bytes:
        self._ensure_login()
        dl_url = f"{self.base_url}/forum/dl.php?t={topic_id}"
        resp = self.scraper.get(dl_url, allow_redirects=False)

        if resp.is_redirect and "login.php" in resp.headers.get("Location", ""):
            log.warning("Download redirect to login — re-authenticating...")
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