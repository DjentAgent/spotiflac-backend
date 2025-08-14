import json
import logging
import re
import uuid
import os
import time
from concurrent.futures import ThreadPoolExecutor
from math import ceil
from typing import List, Optional, Tuple
from urllib.parse import urljoin, parse_qs

import bencodepy
import cloudscraper
import lxml.html
import redis

from fastapi import HTTPException
from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

log = logging.getLogger(__name__)

# Регулярные выражения для определения lossless/lossy
_LOSSLESS_RE = re.compile(r"\b(flac|wavpack|wv|ape|alac|aiff|pcm|dts|mlp|tta|mqa|lossless)\b", re.IGNORECASE)
_LOSSY_RE = re.compile(r"\b(mp3|aac|ogg|opus|lossy)\b", re.IGNORECASE)
_PG_BASE_URL_RE = re.compile(r"PG_BASE_URL\s*:\s*['\"][^?]+\?([^'\"]+)['\"]", re.IGNORECASE)
_TOTAL_RE = re.compile(r"Результатов поиска:\s*(\d+)", re.IGNORECASE)
_FORM_TOKEN_RE = re.compile(r"form_token\s*:\s*'([0-9a-f]+)'", re.IGNORECASE)


class CaptchaRequired(Exception):
    """Используется для индикации того, что RuTracker запросил CAPTCHA."""

    def __init__(self, session_id: str, img_url: str):
        super().__init__(f"Captcha required: {session_id}")
        self.session_id = session_id
        self.img_url = img_url


class RutrackerService:
    """Сервис для взаимодействия с форумом RuTracker.

    Поддерживает аутентификацию, поиск, фильтрацию, получение файловых
    списков и скачивание `.torrent`-файлов. Для оптимизации при фильтрации
    по имени трека сначала пытается получить список файлов из HTML, а затем
    при необходимости скачивает торрент.
    """

    def __init__(self, base_url: Optional[str] = None):
        self.base_url = (base_url or settings.rutracker_base).rstrip("/")
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False},
            delay=10
        )
        self.redis = redis.Redis.from_url(settings.redis_url)
        self.cookie_ttl = getattr(settings, "rutracker_cookie_ttl", 24 * 3600)
        self.filelist_ttl = getattr(settings, "rutracker_filelist_ttl", 24 * 3600)
        self.blob_ttl = getattr(settings, "rutracker_blob_ttl", 7 * 24 * 3600)
        self.search_ttl = getattr(settings, "rutracker_search_ttl", 5 * 60)
        self.dump_dir = getattr(settings, "debug_html_dir", "/app/debug_html")
        self._last_html: Optional[str] = None

        log.debug("Init with base_url=%s", self.base_url)
        try:
            raw = self.redis.get("rutracker:cookiejar")
            if raw:
                cookies = json.loads(raw)
                self.scraper.cookies.update(cookies)
                log.debug("Restored cookies bb_session=%s", cookies.get("bb_session"))
        except Exception:
            log.exception("Cookie restore failed")

    # ------------------------------------------------------------------
    # Вспомогательные методы
    # ------------------------------------------------------------------
    def _extract_form_token(self, html: str) -> str:
        m = _FORM_TOKEN_RE.search(html)
        return m.group(1) if m else ""

    def _follow_normal_redirect(self, resp):
        """Если это обычный 30x (НЕ на login.php) — проходим редирект и возвращаем конечный ответ."""
        try:
            if resp is None:
                return resp
            if resp.status_code in (301, 302, 303, 307, 308):
                loc = resp.headers.get("Location") or ""
                if loc and "login.php" not in loc:
                    url = urljoin(self.base_url + "/forum/", loc)
                    r2 = self.scraper.get(url, allow_redirects=True)
                    # если и тут редирект — requests сам догрызёт благодаря allow_redirects=True
                    return r2
        except Exception as e:
            log.debug("Failed to follow normal redirect: %s", e)
        return resp

    # ------------------------------------------------------------------
    # Логин
    # ------------------------------------------------------------------
    # --- внутри RutrackerService ---

    def _login_sync(self):
        login_url = f"{self.base_url}/forum/login.php"
        resp = self.scraper.get(login_url);
        resp.raise_for_status()
        html = resp.text;
        self._last_html = html

        try:
            doc = lxml.html.fromstring(html)
            form = doc.get_element_by_id("login-form-full")
        except Exception as e:
            os.makedirs(self.dump_dir, exist_ok=True)
            dump = os.path.join(self.dump_dir, f"login_error_{int(time.time())}.html")
            with open(dump, "w", encoding="utf-8") as f:
                f.write(html)
            log.error("Login parsing failed, dumped HTML to %s", dump, exc_info=e)
            raise RuntimeError(f"Login parsing failed — HTML dumped to {dump}") from e

        data = {
            inp.get("name"): inp.get("value", "")
            for inp in form.xpath(".//input[@type='hidden']") if inp.get("name")
        }
        if (token := self._extract_form_token(html)):
            data["form_token"] = token
        data.update({
            "login_username": settings.rutracker_login,
            "login_password": settings.rutracker_password,
            "login": form.xpath(".//input[@type='submit']/@value")[0],
        })

        # делаем логин
        post = self.scraper.post(login_url, data=data, headers={"Referer": login_url}, allow_redirects=True)
        # если пришёл 521/5xx, но cookie выставились — считаем успехом
        try:
            post.raise_for_status()
        except Exception as e:
            jar = self.scraper.cookies.get_dict()
            if jar.get("bb_session"):
                log.warning("Login returned %s but bb_session present -> treating as success",
                            getattr(post, "status_code", "?"))
            else:
                raise

        jar = self.scraper.cookies.get_dict()
        if not jar.get("bb_session"):
            # CAPTCHA flow как у тебя было (без изменений)
            cap = self.scraper.get(login_url);
            cap.raise_for_status()
            cap_html = cap.text;
            self._last_html = cap_html
            cdoc = lxml.html.fromstring(cap_html)
            hidden = {
                inp.get("name"): inp.get("value", "")
                for inp in cdoc.xpath(".//input[@type='hidden']") if inp.get("name")
            }
            sid = hidden.get("cap_sid", uuid.uuid4().hex)
            imgs = cdoc.xpath("//img[contains(@src,'/captcha/')]/@src")
            img_url = urljoin(self.base_url + "/forum/", imgs[0]) if imgs else login_url

            code_fields = cdoc.xpath(".//input[starts-with(@name,'cap_code_')]/@name")
            if not code_fields:
                dump = os.path.join(self.dump_dir, f"captcha_parse_error_{sid}_{int(time.time())}.html")
                os.makedirs(self.dump_dir, exist_ok=True)
                with open(dump, "w", encoding="utf-8") as f: f.write(cap_html)
                log.error("Captcha parse failed, dumped HTML to %s", dump)
                raise RuntimeError(f"Captcha parse failed — HTML dumped to {dump}")
            code_field = code_fields[0]
            submit_val = cdoc.xpath(".//input[@type='submit']/@value")[0]
            self.redis.set(f"login:{sid}", json.dumps({
                "hidden": hidden,
                "code_field": code_field,
                "submit_val": submit_val,
            }), ex=300)
            raise CaptchaRequired(sid, img_url)

        # «мягкий» прогрев — не рушим, если 52x/антибот
        try:
            warm = self.scraper.get(f"{self.base_url}/forum/tracker.php")
            if warm.status_code >= 500:
                log.warning("Warmup returned %d — continuing (bb_session present)", warm.status_code)
            else:
                warm.raise_for_status()
        except Exception as e:
            log.warning("Warmup exception: %s — continuing (bb_session present)", e)

        self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)
        log.debug("Login succeeded, bb_session=%s", jar.get("bb_session"))

    def _ensure_login(self):
        # если кука есть — не лезем в логин
        if self.scraper.cookies.get("bb_session"):
            return
        try:
            self._login_sync()
        except CaptchaRequired:
            # пробрасываем, пусть верхний уровень обработает
            raise
        except Exception as e:
            # если внезапно после попытки логина кука появилась — продолжаем
            if self.scraper.cookies.get("bb_session"):
                log.warning("ensure_login: caught %s but bb_session present -> continue", e)
                return
            raise

    def initiate_login(self) -> Tuple[Optional[str], Optional[str]]:
        login_url = f"{self.base_url}/forum/login.php"
        resp = self.scraper.get(login_url);
        resp.raise_for_status()
        doc = lxml.html.fromstring(resp.text)
        form = doc.get_element_by_id("login-form-full")
        hidden = {
            inp.get("name"): inp.get("value", "")
            for inp in form.xpath(".//input[@type='hidden']") if inp.get("name")
        }
        sid = hidden.get("cap_sid")
        imgs = doc.xpath("//img[contains(@src,'/captcha/')]/@src")
        if not sid or not imgs:
            return None, None
        code_field = form.xpath(".//input[starts-with(@name,'cap_code_')]/@name")[0]
        submit_val = form.xpath(".//input[@type='submit']/@value")[0]
        session_id = uuid.uuid4().hex
        self.redis.set(f"login:{session_id}", json.dumps({
            "hidden": hidden,
            "code_field": code_field,
            "submit_val": submit_val
        }), ex=300)
        img_url = urljoin(self.base_url + "/forum/", imgs[0])
        raise CaptchaRequired(session_id, img_url)

    def complete_login(self, session_id: str, solution: str):
        raw = self.redis.get(f"login:{session_id}")
        if not raw:
            raise RuntimeError("Invalid or expired captcha session")
        info = json.loads(raw)
        data = info["hidden"].copy()
        data.update({
            "login_username": settings.rutracker_login,
            "login_password": settings.rutracker_password,
            "cap_sid": info["hidden"]["cap_sid"],
            info["code_field"]: solution,
            "login": info["submit_val"],
        })
        login_url = f"{self.base_url}/forum/login.php"
        r = self.scraper.post(login_url, data=data, headers={"Referer": login_url}, allow_redirects=False)
        loc = r.headers.get("Location")
        if loc and r.status_code in (302, 303):
            self.scraper.get(urljoin(self.base_url + "/forum/", loc)).raise_for_status()
        else:
            r.raise_for_status()
        # прогрев после CAPTCHA
        self.scraper.get(f"{self.base_url}/forum/tracker.php").raise_for_status()
        jar = self.scraper.cookies.get_dict()
        if not jar.get("bb_session"):
            raise RuntimeError("Login failed after captcha")
        self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)

    # ------------------------------------------------------------------
    # Работа с торрент-файлами и списоком файлов
    # ------------------------------------------------------------------
    def _get_torrent_blob(self, tid: int) -> bytes:
        key = f"torrentblob:{tid}"
        if (blob := self.redis.get(key)):
            return blob
        data = self._download_sync(tid)
        self.redis.set(key, data, ex=self.blob_ttl)
        return data

    def _parse_filelist(self, bts: bytes) -> List[str]:
        meta = bencodepy.decode(bts)[b"info"]
        if b"filename" in meta:
            return [meta[b"filename"].decode(errors="ignore")]
        paths = []
        for f in meta.get(b"files", []):
            parts = [p.decode(errors="ignore") for p in f[b"path"]]
            paths.append("/".join(parts))
        return paths

    # ------------------------------------------------------------------
    # HTML file list parsing
    # ------------------------------------------------------------------
    def _parse_filetree_html(self, root: lxml.html.HtmlElement) -> List[str]:
        """Собирает список файлов из HTML-дерева RuTracker.

        На странице раздачи RuTracker размещает список файлов в виде
        вложенного `<ul class="ftree">`.  Однако класс `ftree` встречается
        также у контейнеров вроде `<div class="ftree-windowed">`.  Поэтому
        для поиска корня списка файлов нужно искать именно `ul.ftree`.
        Далее метод рекурсивно обходит все `li`, собирая имена файлов и
        директорий.
        """
        result: List[str] = []

        def recurse(ul: lxml.html.HtmlElement, prefix: str) -> None:
            for li in ul.xpath('./li'):
                divs = li.xpath('./div')
                if not divs:
                    continue
                div = divs[0]
                name_el = div.xpath('.//b/text()')
                name = name_el[0].strip() if name_el else div.text_content().strip()
                path = f"{prefix}/{name}" if prefix else name
                sub_uls = li.xpath('./ul')
                if sub_uls:
                    recurse(sub_uls[0], path)
                else:
                    result.append(path)

        trees = root.xpath(
            ".//ul[contains(concat(' ', normalize-space(@class), ' '), ' ftree ')]"
        )
        if not trees:
            return []
        recurse(trees[0], '')
        return result

    def _fetch_filelist_html(self, tid: int, cat_id: Optional[str] = None) -> Optional[str]:
        """Получает HTML со списком файлов через ``viewtorrent.php``.

        Для разных версий RuTracker API могут использоваться разные
        параметры: ``t`` (topic ID) сам по себе, ``cat`` или ``cat_id``.
        Этот метод по очереди пытается несколько комбинаций POST-параметров
        и, если POST возвращает пустую страницу, аналогичный GET-запрос.
        Возвращает первую непустую строку HTML или ``None``.
        """
        self._ensure_login()
        url = f"{self.base_url}/forum/viewtorrent.php"
        referer = f"{self.base_url}/forum/viewtopic.php?t={tid}"
        headers = {
            'Referer': referer,
            'X-Requested-With': 'XMLHttpRequest',
            # Запрашиваем только gzip/deflate, чтобы сервер не использовал zstd
            'Accept-Encoding': 'gzip, deflate',
            # Указываем Origin, как делает браузер
            'Origin': self.base_url,
            'Accept': '*/*',
        }
        # prepare candidate data dictionaries
        candidates: List[dict] = []
        # always try with only t
        candidates.append({'t': str(tid)})
        # if category is known, try both 'cat' and 'cat_id'
        if cat_id:
            candidates.append({'t': str(tid), 'cat': str(cat_id)})
            candidates.append({'t': str(tid), 'cat_id': str(cat_id)})
        # attempt each candidate
        for data in candidates:
            try:
                # используем form-urlencoded
                resp = self.scraper.post(url, data=data, headers=headers, allow_redirects=True)
                resp.raise_for_status()
                text = resp.text or ''
                if text.strip():
                    log.debug("viewtorrent POST with %s returned body of length %d for tid %s", data, len(text), tid)
                    return text
                # try GET as fallback
                get_resp = self.scraper.get(url, params=data, headers=headers, allow_redirects=True)
                get_resp.raise_for_status()
                text = get_resp.text or ''
                if text.strip():
                    log.debug("viewtorrent GET with %s returned body of length %d for tid %s", data, len(text), tid)
                    return text
            except Exception as exc:
                log.debug("viewtorrent request with %s failed for tid %s: %s", data, tid, exc)
                continue
        return None

    def _get_filelist_from_page(self, tid: int) -> Optional[List[str]]:
        """Пытается получить список файлов, парся HTML.

        Сначала пробует viewtorrent.php без категории.  Затем загружает
        страницу раздачи, пытается найти cat_id в скриптах и снова
        запрашивает viewtorrent.php.  В конце ищет уже загруженный список
        в `<div id="tor-filelist">`.  Возвращает None, если ничего не
        найдено.
        """
        # viewtorrent без категории
        html = self._fetch_filelist_html(tid)
        if html:
            try:
                # wrap the fragment in a container to ensure a single root
                frag = f"<div>{html}</div>"
                doc = lxml.html.fromstring(frag)
                files = self._parse_filetree_html(doc)
                if files:
                    log.debug("viewtorrent.php returned %d file entries for tid %s", len(files), tid)
                    return files
            except Exception as e:
                log.debug("Parsing filelist HTML via viewtorrent failed for tid %s: %s", tid, e)
        # загрузка страницы раздачи
        try:
            topic_url = f"{self.base_url}/forum/viewtopic.php?t={tid}"
            resp = self.scraper.get(topic_url)
            resp.raise_for_status()
            page_html = resp.text
            doc = lxml.html.fromstring(page_html)
            cat_id = None
            m = re.search(r"cat_id\s*:\s*'(?P<id>\d+)'", page_html)
            if m:
                cat_id = m.group('id')
                html2 = self._fetch_filelist_html(tid, cat_id)
                if html2:
                    try:
                        frag2 = f"<div>{html2}</div>"
                        doc2 = lxml.html.fromstring(frag2)
                        files2 = self._parse_filetree_html(doc2)
                        if files2:
                            log.debug("viewtorrent.php (cat=%s) returned %d file entries for tid %s", cat_id,
                                      len(files2), tid)
                            return files2
                    except Exception as e:
                        log.debug("Parsing filelist HTML via viewtorrent with cat_id failed for tid %s: %s", tid, e)
            # поиск загруженного tor-filelist
            filelist_div = doc.xpath("//div[@id='tor-filelist']")
            if filelist_div:
                try:
                    files3 = self._parse_filetree_html(filelist_div[0])
                    if files3:
                        log.debug("viewtopic page contained %d file entries for tid %s", len(files3), tid)
                        return files3
                except Exception as e:
                    log.debug("Parsing preloaded filelist failed for tid %s: %s", tid, e)
        except CaptchaRequired:
            raise
        except Exception as e:
            log.debug("Failed to fetch or parse topic page for tid %s: %s", tid, e)
        return None

    def _get_filelist(self, tid: int) -> List[str]:
        """Возвращает список файлов для указанного torrent ID.

        Сначала пытается взять список файлов из кэша.  Затем — парсит
        HTML-страницу раздачи; если удаётся, использует этот список.  В
        противном случае скачивает и декодирует `.torrent` файл.  Каждый
        шаг сопровождается логом, сообщающим, откуда был получен список.
        """
        key = f"tracklist:{tid}"
        raw = self.redis.get(key)
        if raw:
            try:
                return json.loads(raw)
            except Exception:
                pass
        # пробуем получить список из HTML
        fl: Optional[List[str]] = None
        try:
            fl = self._get_filelist_from_page(tid)
        except CaptchaRequired:
            raise
        if fl:
            log.info("Using file list from HTML for tid %s with %d entries", tid, len(fl))
            self.redis.set(key, json.dumps(fl), ex=self.filelist_ttl)
            return fl
        # fallback: список из torrent файла
        try:
            log.debug("Falling back to torrent file download for tid %s", tid)
            blob = self._get_torrent_blob(tid)
            fl = self._parse_filelist(blob)
            log.info("Using file list from torrent file for tid %s with %d entries", tid, len(fl))
        except CaptchaRequired:
            raise
        except Exception as e:
            log.debug("Failed to parse torrent blob for tid %s: %s", tid, e)
            fl = []
            log.info("Using file list from torrent file for tid %s with %d entries", tid, len(fl))
        self.redis.set(key, json.dumps(fl), ex=self.filelist_ttl)
        return fl

    # ------------------------------------------------------------------
    # Поиск раздач
    # ------------------------------------------------------------------
    def _search_sync(self, query: str, only_lossless: Optional[bool], track: Optional[str]) -> List[TorrentInfo]:
        cache_key = f"search:{query}:{only_lossless}:{track}"
        search_url = f"{self.base_url}/forum/tracker.php"
        self._ensure_login()

        def _is_transient(status: int) -> bool:
            # всё, что часто бывает на первом заходе / под антиботом
            return status in (429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 525, 526)

        def _retry(op, attempts: int = 3, backoff: float = 0.6):
            last_exc = None
            for i in range(attempts):
                try:
                    return op()
                except Exception as e:
                    last_exc = e
                    time.sleep(backoff * (i + 1))
            if last_exc:
                raise last_exc

        def guarded_get(params):
            def _do():
                r = self.scraper.get(search_url, params=params, allow_redirects=False)
                # редирект на логин?
                if r.status_code in (301, 302) and 'login.php' in (r.headers.get('Location') or ''):
                    log.debug("Session expired on search GET, re-login")
                    self.scraper.cookies.clear()
                    self.redis.delete("rutracker:cookiejar")
                    self._login_sync()
                    r = self.scraper.get(search_url, params=params, allow_redirects=False)
                # обычный редирект — пройти
                r = self._follow_normal_redirect(r)
                # транзиент? бросаем исключение, чтобы _retry повторил
                if _is_transient(getattr(r, "status_code", 0)):
                    raise RuntimeError(f"Transient GET status {r.status_code}")
                r.raise_for_status()
                return r

            return _retry(_do, attempts=3, backoff=0.7)

        def guarded_post(data):
            def _do():
                r = self.scraper.post(search_url, data=data, allow_redirects=False)
                if r.status_code in (301, 302) and 'login.php' in (r.headers.get('Location') or ''):
                    log.debug("Session expired on search POST, re-login")
                    self.scraper.cookies.clear()
                    self.redis.delete("rutracker:cookiejar")
                    self._login_sync()
                    r = self.scraper.post(search_url, data=data, allow_redirects=False)
                r = self._follow_normal_redirect(r)
                if _is_transient(getattr(r, "status_code", 0)):
                    raise RuntimeError(f"Transient POST status {r.status_code}")
                r.raise_for_status()
                return r

            return _retry(_do, attempts=3, backoff=0.7)

        # ---------- GET (форма/токен) ----------
        try:
            r0 = guarded_get({"nm": query})
        except Exception as e:
            log.warning("Search GET failed after retries: %s", e)
            raise HTTPException(status_code=502, detail="Rutracker GET search failed (temporary)")

        html0 = r0.text or ""
        if 'id="login-form-full"' in html0:
            # бывает, если антибот подсунул логин ещё раз
            log.debug("Search GET returned login page, re-login")
            self.scraper.cookies.clear()
            self.redis.delete("rutracker:cookiejar")
            self._login_sync()
            try:
                r0 = guarded_get({"nm": query})
            except Exception as e:
                log.warning("Search GET (after re-login) failed: %s", e)
                raise HTTPException(status_code=502, detail="Rutracker GET search failed (after login)")

        self._last_html = r0.text
        token = self._extract_form_token(r0.text)
        post_data = {"nm": query, "f[]": "-1"}
        if token:
            post_data["form_token"] = token

        # ---------- POST (результаты) ----------
        try:
            r1 = guarded_post(post_data)
        except Exception as e:
            log.warning("Search POST failed after retries: %s", e)
            raise HTTPException(status_code=502, detail="Rutracker POST search failed (temporary)")

        html1 = r1.text or ""
        if 'id="login-form-full"' in html1:
            log.debug("Search POST returned login page, re-login")
            self.scraper.cookies.clear()
            self.redis.delete("rutracker:cookiejar")
            self._login_sync()
            try:
                r1 = guarded_post(post_data)
            except Exception as e:
                log.warning("Search POST (after re-login) failed: %s", e)
                raise HTTPException(status_code=502, detail="Rutracker POST search failed (after login)")

        # ---------- парсинг ----------
        doc = lxml.html.fromstring(r1.text)
        parsed: List[Tuple[TorrentInfo, int]] = []
        for row in doc.xpath("//table[@id='tor-tbl']//tr[@data-topic_id]"):
            hrefs = row.xpath(".//a[contains(@href,'dl.php?t=')]/@href")
            if not hrefs:
                continue
            tid = int(row.get("data-topic_id"))
            forum_txt = (row.xpath(".//td[contains(@class,'f-name-col')]//a/text()") or [""])[0].strip()
            title_txt = (row.xpath(".//td[contains(@class,'t-title-col')]//a/text()") or [""])[0].strip()
            combined = f"{forum_txt} {title_txt}".strip()
            is_l = bool(_LOSSLESS_RE.search(combined))
            is_y = bool(_LOSSY_RE.search(combined))
            if only_lossless is True and (not is_l or is_y): continue
            if only_lossless is False and is_l: continue
            size = (row.xpath(".//td[contains(@class,'tor-size')]//a/text()") or [""])[0].strip()
            se = int((row.xpath(".//b[contains(@class,'seedmed')]/text()") or ["0"])[0].strip())
            le = int((row.xpath(".//td[contains(@class,'leechmed')]/text()") or ["0"])[0].strip())
            url_dl = urljoin(self.base_url + "/forum/", hrefs[0])
            parsed.append((TorrentInfo(
                title=combined, url=url_dl, size=size, seeders=se, leechers=le
            ), tid))

        # фильтр по треку
        if track:
            from rapidfuzz import fuzz as rf_fuzz
            results = []
            t_low = track.lower()
            for ti, tid in parsed:
                if t_low in ti.title.lower():
                    results.append(ti)
                    continue
                try:
                    files = self._get_filelist(tid)
                except CaptchaRequired:
                    continue
                if any(t_low in f.lower() or rf_fuzz.partial_ratio(t_low, f.lower()) >= 80 for f in files):
                    results.append(ti)
            final = results
        else:
            final = [ti for ti, _ in parsed]

        # кеш
        to_cache = [
            {"title": r.title, "url": r.url, "size": r.size,
             "seeders": r.seeders, "leechers": r.leechers}
            for r in final
        ]
        self.redis.set(cache_key, json.dumps(to_cache), ex=self.search_ttl)
        return final

    async def search(self, query, only_lossless=None, track=None):
        import asyncio
        return await asyncio.to_thread(self._search_sync, query, only_lossless, track)

    # ------------------------------------------------------------------
    # Скачивание торрент-файла
    # ------------------------------------------------------------------
    def _download_sync(self, topic_id: int) -> bytes:
        self._ensure_login()
        dl_url = f"{self.base_url}/forum/dl.php?t={topic_id}"
        resp = self.scraper.get(dl_url, allow_redirects=True)
        resp.raise_for_status()
        ctype = resp.headers.get("Content-Type", "")
        if "application/x-bittorrent" in ctype:
            return resp.content
        # HTML вместо торрента
        html = resp.text;
        self._last_html = html
        os.makedirs(self.dump_dir, exist_ok=True)
        path = os.path.join(self.dump_dir, f"download_error_{topic_id}_{int(time.time())}.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        log.error("Download failed, dumped HTML to %s", path)
        if "исчерпали суточный лимит" in html:
            raise HTTPException(status_code=429, detail="Daily download limit exceeded (1000/day)")
        doc = lxml.html.fromstring(html)
        hidden = {inp.get("name"): inp.get("value", "") for inp in doc.xpath(".//input[@type='hidden']") if
                  inp.get("name")}
        code_fields = doc.xpath(".//input[starts-with(@name,'cap_code_')]/@name")
        imgs = doc.xpath("//img[contains(@src,'/captcha/')]/@src")
        if not code_fields or not imgs:
            raise RuntimeError(f"Download parsing failed — HTML dumped to {path}")
        sid = hidden.get("cap_sid", uuid.uuid4().hex)
        img_url = urljoin(self.base_url + "/forum/", imgs[0])
        code_field = code_fields[0]
        submit_val = doc.xpath(".//input[@type='submit']/@value")[0]
        self.redis.set(f"login:{sid}", json.dumps({
            "hidden": hidden,
            "code_field": code_field,
            "submit_val": submit_val,
        }), ex=300)
        raise CaptchaRequired(sid, img_url)

    async def download(self, topic_id: int):
        import asyncio
        return await asyncio.to_thread(self._download_sync, topic_id)

    async def close(self):
        pass
