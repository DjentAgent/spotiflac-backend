import json
import logging
import re
import uuid
import os
import time
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache, wraps
from typing import List, Optional, Tuple, Dict, Any
from urllib.parse import urljoin, parse_qs
from datetime import datetime, timedelta

import bencodepy
import cloudscraper
import lxml.html
import redis
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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


def retry_on_transient(max_attempts=3, backoff_factor=0.5):
    """Декоратор для retry логики при транзиентных ошибках."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, TimeoutError) as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        sleep_time = backoff_factor * (2 ** attempt)
                        log.debug(f"Retry {attempt + 1}/{max_attempts} after {sleep_time}s: {e}")
                        time.sleep(sleep_time)
                except CaptchaRequired:
                    raise  # Не retry для CAPTCHA
                except Exception as e:
                    if "transient" in str(e).lower() or "temporary" in str(e).lower():
                        last_exception = e
                        if attempt < max_attempts - 1:
                            sleep_time = backoff_factor * (2 ** attempt)
                            time.sleep(sleep_time)
                    else:
                        raise
            if last_exception:
                raise last_exception

        return wrapper

    return decorator


class SessionKeepAlive:
    """Фоновый поток для поддержания сессии в живом состоянии."""

    def __init__(self, service):
        self.service = service
        self.running = False
        self.thread = None
        self.interval = 300  # 5 минут

    def start(self):
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()
            log.info("Session keepalive thread started")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
            log.info("Session keepalive thread stopped")

    def _run(self):
        while self.running:
            try:
                # Ждем интервал
                for _ in range(self.interval):
                    if not self.running:
                        break
                    time.sleep(1)

                if not self.running:
                    break

                # Пингуем сессию
                self.service._ping_session()

            except Exception as e:
                log.error(f"Keepalive error: {e}")


class RutrackerService:
    """Сервис для взаимодействия с форумом RuTracker.

    Поддерживает аутентификацию, поиск, фильтрацию, получение файловых
    списков и скачивание `.torrent`-файлов. Оптимизирован для стабильной
    работы с автоматическим поддержанием сессии.
    """

    _instance = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """Singleton паттерн для единственного экземпляра сервиса."""
        if not cls._instance:
            with cls._instance_lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, base_url: Optional[str] = None):
        # Проверяем, инициализирован ли уже экземпляр
        if hasattr(self, '_initialized'):
            return
        self._initialized = True

        self.base_url = (base_url or settings.rutracker_base).rstrip("/")

        # Настройка scraper с connection pooling и retry
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False},
            delay=10
        )

        # Настройка connection pooling и retry logic
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=Retry(
                total=3,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504, 520, 521, 522, 523, 524, 525, 526]
            )
        )
        self.scraper.mount("http://", adapter)
        self.scraper.mount("https://", adapter)

        # Redis
        self.redis = redis.Redis.from_url(settings.redis_url, decode_responses=False)

        # Настройки TTL
        self.cookie_ttl = getattr(settings, "rutracker_cookie_ttl", 24 * 3600)
        self.filelist_ttl = getattr(settings, "rutracker_filelist_ttl", 24 * 3600)
        self.blob_ttl = getattr(settings, "rutracker_blob_ttl", 7 * 24 * 3600)
        self.search_ttl = getattr(settings, "rutracker_search_ttl", 5 * 60)
        self.dump_dir = getattr(settings, "debug_html_dir", "/app/debug_html")

        # Thread safety
        self._login_lock = threading.Lock()
        self._login_in_progress = False
        self._last_session_check = 0
        self._session_check_interval = 60  # Проверка валидности сессии раз в минуту

        # Thread pool для параллельных операций
        self._executor = ThreadPoolExecutor(max_workers=10)

        # Последний HTML для отладки
        self._last_html: Optional[str] = None

        # Keepalive thread
        self._keepalive = SessionKeepAlive(self)

        log.debug("Init with base_url=%s", self.base_url)

        # Восстановление cookies из Redis
        try:
            raw = self.redis.get("rutracker:cookiejar")
            if raw:
                cookies = json.loads(raw)
                self.scraper.cookies.update(cookies)
                log.debug("Restored cookies bb_session=%s", cookies.get("bb_session"))
        except Exception:
            log.exception("Cookie restore failed")

        # Запускаем keepalive thread
        self._keepalive.start()

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
                    r2 = self.scraper.get(url, allow_redirects=True, timeout=30)
                    return r2
        except Exception as e:
            log.debug("Failed to follow normal redirect: %s", e)
        return resp

    def _is_logged_in_response(self, response) -> bool:
        """Проверяет, что ответ указывает на залогиненного пользователя."""
        if response.status_code in (301, 302):
            loc = response.headers.get("Location", "")
            if "login.php" in loc:
                return False

        if response.status_code == 200:
            text = response.text[:5000]  # Проверяем только начало для скорости
            if 'id="login-form-full"' in text or 'name="login_username"' in text:
                return False
            # Проверяем наличие элементов, доступных только залогиненным
            if 'href="./login.php?logout=' in text or 'class="logged-in-as"' in text:
                return True

        return True  # По умолчанию считаем валидным

    def _ping_session(self):
        """Пингует сессию для поддержания в живом состоянии."""
        try:
            if not self.scraper.cookies.get("bb_session"):
                log.debug("No session to ping")
                return

            # Легкий запрос на главную
            url = f"{self.base_url}/forum/index.php"
            resp = self.scraper.get(url, timeout=10, allow_redirects=False)

            if not self._is_logged_in_response(resp):
                log.info("Session expired during ping, clearing cookies")
                self.scraper.cookies.clear()
                self.redis.delete("rutracker:cookiejar")
            else:
                log.debug("Session ping successful")
                # Обновляем TTL cookies в Redis
                jar = self.scraper.cookies.get_dict()
                if jar.get("bb_session"):
                    self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)

        except Exception as e:
            log.error(f"Session ping failed: {e}")

    # ------------------------------------------------------------------
    # Логин с улучшенной проверкой валидности
    # ------------------------------------------------------------------

    def _validate_session(self) -> bool:
        """Проверяет валидность текущей сессии."""
        if not self.scraper.cookies.get("bb_session"):
            return False

        # Кешируем результат проверки на минуту
        now = time.time()
        if now - self._last_session_check < self._session_check_interval:
            return True

        try:
            # Проверочный запрос
            test_url = f"{self.base_url}/forum/index.php"
            resp = self.scraper.get(test_url, timeout=10, allow_redirects=False)

            is_valid = self._is_logged_in_response(resp)

            if is_valid:
                self._last_session_check = now
                log.debug("Session validation successful")
            else:
                log.info("Session validation failed")

            return is_valid

        except Exception as e:
            log.warning(f"Session validation error: {e}")
            return False

    @retry_on_transient(max_attempts=3)
    def _login_sync(self):
        """Выполняет синхронный логин с retry логикой."""
        login_url = f"{self.base_url}/forum/login.php"

        # GET для получения формы
        resp = self.scraper.get(login_url, timeout=30)
        resp.raise_for_status()
        html = resp.text
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

        # Собираем данные формы
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

        # POST логин
        post = self.scraper.post(login_url, data=data, headers={"Referer": login_url},
                                 allow_redirects=True, timeout=30)

        # Проверяем результат
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
            # CAPTCHA flow
            self._handle_login_captcha()

        # Прогрев сессии
        self._warmup_session()

        # Сохраняем cookies
        jar = self.scraper.cookies.get_dict()
        self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)
        self._last_session_check = time.time()
        log.debug("Login succeeded, bb_session=%s", jar.get("bb_session"))

    def _handle_login_captcha(self):
        """Обработка CAPTCHA при логине."""
        login_url = f"{self.base_url}/forum/login.php"
        cap = self.scraper.get(login_url, timeout=30)
        cap.raise_for_status()
        cap_html = cap.text
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
            with open(dump, "w", encoding="utf-8") as f:
                f.write(cap_html)
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

    def _warmup_session(self):
        """Прогрев сессии после логина."""
        try:
            warm = self.scraper.get(f"{self.base_url}/forum/tracker.php", timeout=30)
            if warm.status_code >= 500:
                log.warning("Warmup returned %d — continuing (bb_session present)", warm.status_code)
            else:
                warm.raise_for_status()
        except Exception as e:
            log.warning("Warmup exception: %s — continuing (bb_session present)", e)

    def _ensure_login(self):
        """Обеспечивает наличие валидной сессии с защитой от race conditions."""
        # Быстрая проверка без блокировки
        if self._validate_session():
            return

        # Нужен логин - используем блокировку
        with self._login_lock:
            # Если другой поток уже логинится, ждем
            if self._login_in_progress:
                timeout = 30
                start = time.time()
                while self._login_in_progress and (time.time() - start) < timeout:
                    time.sleep(0.1)

                # После ожидания проверяем сессию еще раз
                if self._validate_session():
                    return

            # Еще раз проверяем после получения блокировки
            if self._validate_session():
                return

            # Помечаем начало логина
            self._login_in_progress = True

        try:
            # Очищаем старые cookies
            self.scraper.cookies.clear()
            self.redis.delete("rutracker:cookiejar")

            # Выполняем логин
            self._login_sync()

        except CaptchaRequired:
            raise
        except Exception as e:
            # Проверяем, может кука появилась после ошибки
            if self.scraper.cookies.get("bb_session"):
                log.warning("Login error but bb_session present: %s", e)
                return
            raise
        finally:
            with self._login_lock:
                self._login_in_progress = False

    def complete_login(self, session_id: str, solution: str):
        """Завершает логин после решения CAPTCHA."""
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
        r = self.scraper.post(login_url, data=data, headers={"Referer": login_url},
                              allow_redirects=False, timeout=30)

        loc = r.headers.get("Location")
        if loc and r.status_code in (302, 303):
            self.scraper.get(urljoin(self.base_url + "/forum/", loc), timeout=30).raise_for_status()
        else:
            r.raise_for_status()

        # Прогрев после CAPTCHA
        self._warmup_session()

        jar = self.scraper.cookies.get_dict()
        if not jar.get("bb_session"):
            raise RuntimeError("Login failed after captcha")

        self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)
        self._last_session_check = time.time()

    # ------------------------------------------------------------------
    # Работа с торрент-файлами и списками файлов (оптимизированная)
    # ------------------------------------------------------------------

    def _get_torrent_blob(self, tid: int) -> bytes:
        """Получает blob торрент-файла с кешированием."""
        key = f"torrentblob:{tid}"

        # Используем pipeline для атомарности
        pipe = self.redis.pipeline()
        pipe.get(key)
        pipe.ttl(key)
        blob, ttl = pipe.execute()

        if blob:
            # Обновляем TTL если скоро истекает
            if ttl and ttl < 3600:  # меньше часа
                self.redis.expire(key, self.blob_ttl)
            return blob

        data = self._download_sync(tid)
        self.redis.set(key, data, ex=self.blob_ttl)
        return data

    def _parse_filelist(self, bts: bytes) -> List[str]:
        """Парсит список файлов из torrent blob."""
        try:
            meta = bencodepy.decode(bts)[b"info"]
            if b"name" in meta and b"files" not in meta:
                # Одиночный файл
                return [meta[b"name"].decode(errors="ignore")]

            paths = []
            for f in meta.get(b"files", []):
                parts = [p.decode(errors="ignore") for p in f[b"path"]]
                paths.append("/".join(parts))
            return paths
        except Exception as e:
            log.error(f"Failed to parse torrent blob: {e}")
            return []

    def _parse_filetree_html_optimized(self, root: lxml.html.HtmlElement) -> List[str]:
        """Оптимизированный парсинг списка файлов из HTML."""
        result = []

        # Используем более эффективный XPath
        # Находим все элементы файлов одним запросом
        file_elements = root.xpath(
            ".//ul[contains(@class, 'ftree')]//li[not(.//ul)]//b/text() | "
            ".//ul[contains(@class, 'ftree')]//li[not(.//ul)]//div[not(.//b)]/text()"
        )

        for text in file_elements:
            text = text.strip()
            if text and not text.startswith('├') and not text.startswith('└'):
                result.append(text)

        # Если простой метод не сработал, используем рекурсивный
        if not result:
            def recurse(ul: lxml.html.HtmlElement, prefix: str) -> None:
                for li in ul.xpath('./li'):
                    divs = li.xpath('./div')
                    if not divs:
                        continue
                    div = divs[0]
                    name_el = div.xpath('.//b/text()')
                    name = name_el[0].strip() if name_el else div.text_content().strip()

                    # Очищаем от символов дерева
                    name = re.sub(r'^[├└─\s]+', '', name)

                    path = f"{prefix}/{name}" if prefix else name
                    sub_uls = li.xpath('./ul')
                    if sub_uls:
                        recurse(sub_uls[0], path)
                    else:
                        result.append(path)

            trees = root.xpath(".//ul[contains(@class, 'ftree')]")
            if trees:
                recurse(trees[0], '')

        return result

    @retry_on_transient(max_attempts=2)
    def _fetch_filelist_html(self, tid: int, cat_id: Optional[str] = None) -> Optional[str]:
        """Получает HTML со списком файлов через viewtorrent.php."""
        self._ensure_login()
        url = f"{self.base_url}/forum/viewtorrent.php"
        referer = f"{self.base_url}/forum/viewtopic.php?t={tid}"
        headers = {
            'Referer': referer,
            'X-Requested-With': 'XMLHttpRequest',
            'Accept-Encoding': 'gzip, deflate',
            'Origin': self.base_url,
            'Accept': '*/*',
        }

        candidates: List[dict] = []
        candidates.append({'t': str(tid)})

        if cat_id:
            candidates.append({'t': str(tid), 'cat': str(cat_id)})
            candidates.append({'t': str(tid), 'cat_id': str(cat_id)})

        for data in candidates:
            try:
                resp = self.scraper.post(url, data=data, headers=headers,
                                         allow_redirects=True, timeout=30)
                resp.raise_for_status()
                text = resp.text or ''
                if text.strip():
                    log.debug("viewtorrent POST with %s returned body of length %d for tid %s",
                              data, len(text), tid)
                    return text

                # Fallback to GET
                get_resp = self.scraper.get(url, params=data, headers=headers,
                                            allow_redirects=True, timeout=30)
                get_resp.raise_for_status()
                text = get_resp.text or ''
                if text.strip():
                    log.debug("viewtorrent GET with %s returned body of length %d for tid %s",
                              data, len(text), tid)
                    return text
            except Exception as exc:
                log.debug("viewtorrent request with %s failed for tid %s: %s", data, tid, exc)
                continue

        return None

    def _get_filelist_from_page(self, tid: int) -> Optional[List[str]]:
        """Пытается получить список файлов из HTML страницы."""
        # Сначала пробуем viewtorrent без категории
        html = self._fetch_filelist_html(tid)
        if html:
            try:
                frag = f"<div>{html}</div>"
                doc = lxml.html.fromstring(frag)
                files = self._parse_filetree_html_optimized(doc)
                if files:
                    log.debug("viewtorrent.php returned %d file entries for tid %s", len(files), tid)
                    return files
            except Exception as e:
                log.debug("Parsing filelist HTML via viewtorrent failed for tid %s: %s", tid, e)

        # Загружаем страницу раздачи
        try:
            topic_url = f"{self.base_url}/forum/viewtopic.php?t={tid}"
            resp = self.scraper.get(topic_url, timeout=30)
            resp.raise_for_status()
            page_html = resp.text
            doc = lxml.html.fromstring(page_html)

            # Ищем cat_id
            cat_id = None
            m = re.search(r"cat_id\s*:\s*'(?P<id>\d+)'", page_html)
            if m:
                cat_id = m.group('id')
                html2 = self._fetch_filelist_html(tid, cat_id)
                if html2:
                    try:
                        frag2 = f"<div>{html2}</div>"
                        doc2 = lxml.html.fromstring(frag2)
                        files2 = self._parse_filetree_html_optimized(doc2)
                        if files2:
                            log.debug("viewtorrent.php (cat=%s) returned %d file entries for tid %s",
                                      cat_id, len(files2), tid)
                            return files2
                    except Exception as e:
                        log.debug("Parsing filelist HTML via viewtorrent with cat_id failed for tid %s: %s",
                                  tid, e)

            # Ищем предзагруженный список файлов
            filelist_div = doc.xpath("//div[@id='tor-filelist']")
            if filelist_div:
                try:
                    files3 = self._parse_filetree_html_optimized(filelist_div[0])
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
        """Возвращает список файлов с улучшенным кешированием."""
        # Версионированный кеш
        version_key = "cache_version:filelist"
        current_version = self.redis.get(version_key) or b"1"
        versioned_key = f"tracklist:{tid}:v{current_version.decode()}"

        # Атомарное чтение с TTL
        pipe = self.redis.pipeline()
        pipe.get(versioned_key)
        pipe.ttl(versioned_key)
        raw, ttl = pipe.execute()

        if raw:
            try:
                # Обновляем TTL если скоро истекает
                if ttl and ttl < 300:  # меньше 5 минут
                    self.redis.expire(versioned_key, self.filelist_ttl)
                return json.loads(raw)
            except Exception:
                pass

        # Пробуем получить список из HTML
        fl: Optional[List[str]] = None
        try:
            fl = self._get_filelist_from_page(tid)
        except CaptchaRequired:
            raise

        if fl:
            log.info("Using file list from HTML for tid %s with %d entries", tid, len(fl))
            self.redis.set(versioned_key, json.dumps(fl), ex=self.filelist_ttl)
            return fl

        # Fallback: список из torrent файла
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

        self.redis.set(versioned_key, json.dumps(fl), ex=self.filelist_ttl)
        return fl

    def _check_track_in_files(self, tid: int, track: str) -> bool:
        """Проверяет наличие трека в файлах торрента."""
        try:
            files = self._get_filelist(tid)
            track_lower = track.lower()

            # Быстрая проверка точного вхождения
            for f in files:
                if track_lower in f.lower():
                    return True

            # Нечеткое сравнение для более сложных случаев
            try:
                from rapidfuzz import fuzz as rf_fuzz
                for f in files:
                    if rf_fuzz.partial_ratio(track_lower, f.lower()) >= 80:
                        return True
            except ImportError:
                log.warning("rapidfuzz not installed, using exact match only")

        except CaptchaRequired:
            raise
        except Exception as e:
            log.debug(f"Failed to check track in files for tid {tid}: {e}")

        return False

    # ------------------------------------------------------------------
    # Поиск раздач (оптимизированный с параллельной обработкой)
    # ------------------------------------------------------------------

    @retry_on_transient(max_attempts=3, backoff_factor=0.7)
    def _search_request(self, params: dict, data: dict = None) -> requests.Response:
        """Выполняет поисковый запрос с автоматическим переlogином."""
        search_url = f"{self.base_url}/forum/tracker.php"

        if data:
            # POST запрос
            r = self.scraper.post(search_url, data=data, allow_redirects=False, timeout=30)
        else:
            # GET запрос
            r = self.scraper.get(search_url, params=params, allow_redirects=False, timeout=30)

        # Проверка редиректа на логин
        if r.status_code in (301, 302) and 'login.php' in (r.headers.get('Location') or ''):
            log.debug("Session expired during search, re-login")
            self.scraper.cookies.clear()
            self.redis.delete("rutracker:cookiejar")
            self._login_sync()

            # Повторяем запрос
            if data:
                r = self.scraper.post(search_url, data=data, allow_redirects=False, timeout=30)
            else:
                r = self.scraper.get(search_url, params=params, allow_redirects=False, timeout=30)

        # Обрабатываем обычные редиректы
        r = self._follow_normal_redirect(r)

        # Проверка на транзиентные ошибки
        if r.status_code in (429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 525, 526):
            raise RuntimeError(f"Transient status {r.status_code}")

        r.raise_for_status()

        # Проверка на страницу логина в ответе
        if 'id="login-form-full"' in r.text[:1000]:
            log.debug("Search returned login page, re-login")
            self.scraper.cookies.clear()
            self.redis.delete("rutracker:cookiejar")
            self._login_sync()

            # Повторяем запрос еще раз
            if data:
                r = self.scraper.post(search_url, data=data, allow_redirects=False, timeout=30)
            else:
                r = self.scraper.get(search_url, params=params, allow_redirects=False, timeout=30)

            r = self._follow_normal_redirect(r)
            r.raise_for_status()

        return r

    def _search_sync(self, query: str, only_lossless: Optional[bool], track: Optional[str]) -> List[TorrentInfo]:
        """Синхронный поиск с параллельной фильтрацией."""
        cache_key = f"search:{query}:{only_lossless}:{track}"

        # Проверяем кеш
        cached = self.redis.get(cache_key)
        if cached:
            try:
                items = json.loads(cached)
                return [TorrentInfo(**item) for item in items]
            except Exception:
                pass

        self._ensure_login()

        # GET запрос для получения формы и токена
        try:
            r0 = self._search_request({"nm": query})
        except Exception as e:
            log.warning("Search GET failed: %s", e)
            raise HTTPException(status_code=502, detail="Rutracker GET search failed")

        self._last_html = r0.text
        token = self._extract_form_token(r0.text)

        # POST запрос с результатами
        post_data = {"nm": query, "f[]": "-1"}
        if token:
            post_data["form_token"] = token

        try:
            r1 = self._search_request(params={}, data=post_data)
        except Exception as e:
            log.warning("Search POST failed: %s", e)
            raise HTTPException(status_code=502, detail="Rutracker POST search failed")

        # Парсинг результатов
        doc = lxml.html.fromstring(r1.text)
        parsed: List[Tuple[TorrentInfo, int]] = []

        for row in doc.xpath("//table[@id='tor-tbl']//tr[@data-topic_id]"):
            try:
                hrefs = row.xpath(".//a[contains(@href,'dl.php?t=')]/@href")
                if not hrefs:
                    continue

                tid = int(row.get("data-topic_id"))
                forum_txt = (row.xpath(".//td[contains(@class,'f-name-col')]//a/text()") or [""])[0].strip()
                title_txt = (row.xpath(".//td[contains(@class,'t-title-col')]//a/text()") or [""])[0].strip()
                combined = f"{forum_txt} {title_txt}".strip()

                # Фильтрация по lossless/lossy
                is_lossless = bool(_LOSSLESS_RE.search(combined))
                is_lossy = bool(_LOSSY_RE.search(combined))

                if only_lossless is True and (not is_lossless or is_lossy):
                    continue
                if only_lossless is False and is_lossless:
                    continue

                size = (row.xpath(".//td[contains(@class,'tor-size')]//a/text()") or [""])[0].strip()
                seeders = int((row.xpath(".//b[contains(@class,'seedmed')]/text()") or ["0"])[0].strip())
                leechers = int((row.xpath(".//td[contains(@class,'leechmed')]/text()") or ["0"])[0].strip())
                url_dl = urljoin(self.base_url + "/forum/", hrefs[0])

                parsed.append((TorrentInfo(
                    title=combined,
                    url=url_dl,
                    size=size,
                    seeders=seeders,
                    leechers=leechers
                ), tid))
            except Exception as e:
                log.debug(f"Failed to parse row: {e}")
                continue

        # Фильтрация по треку с параллельной обработкой
        if track:
            results = []
            track_lower = track.lower()

            # Сначала быстрая проверка по названию
            need_file_check = []
            for ti, tid in parsed:
                if track_lower in ti.title.lower():
                    results.append(ti)
                else:
                    need_file_check.append((ti, tid))

            # Параллельная проверка файлов
            if need_file_check:
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = {}
                    for ti, tid in need_file_check:
                        future = executor.submit(self._check_track_in_files, tid, track)
                        futures[future] = ti

                    for future in as_completed(futures):
                        try:
                            if future.result():
                                results.append(futures[future])
                        except CaptchaRequired:
                            # При CAPTCHA прекращаем обработку
                            log.warning("CAPTCHA required during track filtering")
                            break
                        except Exception as e:
                            log.debug(f"Failed to check track in files: {e}")

            final = results
        else:
            final = [ti for ti, _ in parsed]

        # Кешируем результаты
        to_cache = [
            {
                "title": r.title,
                "url": r.url,
                "size": r.size,
                "seeders": r.seeders,
                "leechers": r.leechers
            }
            for r in final
        ]
        self.redis.set(cache_key, json.dumps(to_cache), ex=self.search_ttl)

        return final

    async def search(self, query: str, only_lossless: Optional[bool] = None,
                     track: Optional[str] = None) -> List[TorrentInfo]:
        """Асинхронный поиск."""
        import asyncio
        return await asyncio.to_thread(self._search_sync, query, only_lossless, track)

    # ------------------------------------------------------------------
    # Скачивание торрент-файла
    # ------------------------------------------------------------------

    @retry_on_transient(max_attempts=3)
    def _download_sync(self, topic_id: int) -> bytes:
        """Скачивает торрент-файл."""
        self._ensure_login()

        dl_url = f"{self.base_url}/forum/dl.php?t={topic_id}"
        resp = self.scraper.get(dl_url, allow_redirects=True, timeout=30)
        resp.raise_for_status()

        ctype = resp.headers.get("Content-Type", "")
        if "application/x-bittorrent" in ctype:
            return resp.content

        # HTML вместо торрента - возможно CAPTCHA или лимит
        html = resp.text
        self._last_html = html

        os.makedirs(self.dump_dir, exist_ok=True)
        path = os.path.join(self.dump_dir, f"download_error_{topic_id}_{int(time.time())}.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        log.error("Download failed, dumped HTML to %s", path)

        if "исчерпали суточный лимит" in html:
            raise HTTPException(status_code=429, detail="Daily download limit exceeded (1000/day)")

        # Обработка CAPTCHA
        doc = lxml.html.fromstring(html)
        hidden = {
            inp.get("name"): inp.get("value", "")
            for inp in doc.xpath(".//input[@type='hidden']")
            if inp.get("name")
        }

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

    async def download(self, topic_id: int) -> bytes:
        """Асинхронное скачивание торрент-файла."""
        import asyncio
        return await asyncio.to_thread(self._download_sync, topic_id)

    # ------------------------------------------------------------------
    # Cleanup и закрытие
    # ------------------------------------------------------------------

    async def close(self):
        """Закрывает сервис и освобождает ресурсы."""
        # Останавливаем keepalive
        self._keepalive.stop()

        # Закрываем thread pool
        self._executor.shutdown(wait=False)

        # Сохраняем cookies
        jar = self.scraper.cookies.get_dict()
        if jar.get("bb_session"):
            self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)

        log.info("RutrackerService closed")

    def __del__(self):
        """Деструктор для очистки ресурсов."""
        try:
            if hasattr(self, '_keepalive'):
                self._keepalive.stop()
            if hasattr(self, '_executor'):
                self._executor.shutdown(wait=False)
        except Exception:
            pass


# Singleton getter для FastAPI
@lru_cache()
def get_rutracker_service() -> RutrackerService:
    """Возвращает singleton экземпляр сервиса."""
    return RutrackerService()