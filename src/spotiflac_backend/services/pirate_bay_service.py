import asyncio
import base64
import binascii
import json
import logging
import re
import time
import urllib.parse
from typing import List, Optional, Tuple, Dict, Any, Set
from urllib.parse import urlparse, parse_qs
from contextlib import asynccontextmanager
from functools import lru_cache

import aiohttp
from aiohttp import ClientTimeout, ClientSession, TCPConnector
from aiohttp.client_exceptions import ContentTypeError, ClientError, ServerTimeoutError
from redis.asyncio import Redis, ConnectionPool
from bs4 import BeautifulSoup
from fastapi import HTTPException
from rapidfuzz import fuzz

try:
    import bencodepy
except ImportError:
    try:
        import bencode as bencodepy  # bencode.py package
    except ImportError:
        try:
            import bencoder as bencodepy  # bencoder package
        except ImportError:
            # Fallback на наш минимальный декодер
            from . import bencode_fallback as bencodepy

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

try:
    import libtorrent as lt
except Exception:
    lt = None

log = logging.getLogger(__name__)

# Константы для фильтрации
_LOSSLESS_RE = re.compile(r"\b(flac|wavpack|wv|ape|alac|aiff|pcm|dts|mlp|tta|mqa|lossless)\b", re.IGNORECASE)
_LOSSY_RE = re.compile(r"\b(mp3|aac|ogg|opus|lossy)\b", re.IGNORECASE)


def _human_size(nbytes: int) -> str:
    """Конвертирует байты в человекочитаемый формат."""
    suffixes = ["B", "KB", "MB", "GB", "TB"]
    v = float(nbytes)
    for suf in suffixes:
        if v < 1024.0:
            return f"{v:.2f} {suf}"
        v /= 1024.0
    return f"{v:.2f} PB"


def _hex_upper_from_btih(btih: str) -> Optional[str]:
    """Нормализует BTIH в верхний регистр HEX."""
    v = (btih or "").strip()
    orig = v

    # Убираем префикс URN
    if v.lower().startswith("urn:btih:"):
        v = v.split(":", 2)[-1]
    if v.lower().startswith("urn:btmh:"):
        log.debug("BTIH normalize: got BTMH (v2) '%s' -> unsupported", orig)
        return None

    # HEX формат (40 символов)
    if len(v) == 40 and all(c in "0123456789abcdefABCDEF" for c in v):
        return v.upper()

    # Base32 формат (32 символа)
    if len(v) == 32:
        try:
            raw = base64.b32decode(v.upper())
            return binascii.hexlify(raw).decode("ascii").upper()
        except Exception as e:
            log.debug("BTIH normalize: Base32 decode failed for '%s': %s", orig, e)
            return None

    log.debug("BTIH normalize: unrecognized format '%s'", orig)
    return None


def _slug(s: str) -> str:
    """Создает slug из строки для URL."""
    t = re.sub(r"[^A-Za-z0-9._-]+", "-", s).strip("-")
    return t or "torrent"


def _title_maybe_contains_track(title: str, track: str) -> bool:
    """Быстрая проверка, может ли заголовок содержать трек."""
    if not track:
        return True
    t = (title or "").lower()
    q = track.lower()
    if q in t:
        return True
    return fuzz.partial_ratio(q, t) >= 85


class RetryableSession:
    """HTTP сессия с автоматическими retry и connection pooling."""

    def __init__(self, timeout: float = 20, retries: int = 3):
        self.timeout = timeout
        self.retries = retries
        self.connector = TCPConnector(
            limit=100,
            limit_per_host=30,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            force_close=False,
            keepalive_timeout=30
        )
        self.session: Optional[ClientSession] = None

    async def _ensure_session(self) -> ClientSession:
        if not self.session or self.session.closed:
            self.session = ClientSession(
                connector=self.connector,
                timeout=ClientTimeout(total=self.timeout, connect=10, sock_read=10),
                headers={
                    "Accept": "application/json, text/html;q=0.9,*/*;q=0.8",
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    ),
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                }
            )
        return self.session

    async def request(self, method: str, url: str, **kwargs) -> aiohttp.ClientResponse:
        """Выполняет HTTP запрос с retry логикой."""
        last_error = None

        for attempt in range(self.retries):
            try:
                session = await self._ensure_session()
                async with session.request(method, url, **kwargs) as resp:
                    # Читаем контент сразу, чтобы избежать проблем с закрытием соединения
                    await resp.read()
                    return resp

            except (ServerTimeoutError, asyncio.TimeoutError) as e:
                last_error = e
                if attempt < self.retries - 1:
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    log.debug(f"Retry {attempt + 1}/{self.retries} for {url}: timeout")

            except ClientError as e:
                # Не retry для клиентских ошибок (4xx)
                if hasattr(e, 'status') and 400 <= e.status < 500:
                    raise
                last_error = e
                if attempt < self.retries - 1:
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    log.debug(f"Retry {attempt + 1}/{self.retries} for {url}: {e}")

            except Exception as e:
                last_error = e
                if attempt < self.retries - 1:
                    await asyncio.sleep(0.5 * (2 ** attempt))

        raise last_error or Exception(f"Failed after {self.retries} attempts")

    async def get(self, url: str, **kwargs) -> aiohttp.ClientResponse:
        return await self.request('GET', url, **kwargs)

    async def post(self, url: str, **kwargs) -> aiohttp.ClientResponse:
        return await self.request('POST', url, **kwargs)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
        await self.connector.close()


class ConcurrentLimiter:
    """Управление параллельными запросами с приоритетами."""

    def __init__(self, max_concurrent: int = 20):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.priority_queue: List[Tuple[int, asyncio.Event]] = []
        self.lock = asyncio.Lock()

    @asynccontextmanager
    async def acquire(self, priority: int = 0):
        """Захват семафора с учетом приоритета."""
        event = asyncio.Event()

        async with self.lock:
            self.priority_queue.append((priority, event))
            self.priority_queue.sort(key=lambda x: x[0], reverse=True)

            # Если мы первые в очереди, можем идти
            if self.priority_queue[0][1] == event:
                event.set()

        await event.wait()

        async with self.semaphore:
            try:
                yield
            finally:
                async with self.lock:
                    # Убираем себя из очереди
                    self.priority_queue = [(p, e) for p, e in self.priority_queue if e != event]
                    # Разрешаем следующему
                    if self.priority_queue:
                        self.priority_queue[0][1].set()


class PirateBayService:
    """Оптимизированный сервис для работы с PirateBay API."""

    _instance = None
    _lock = asyncio.Lock()

    def __new__(cls, *args, **kwargs):
        """Singleton паттерн для единственного экземпляра."""
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
            self,
            api_base_url: Optional[str] = None,
            redis_url: Optional[str] = None,
            max_filelist_concurrency: int = None,
            dht_timeout_sec: int = 90,
    ) -> None:
        # Проверяем, инициализирован ли уже
        if hasattr(self, '_initialized'):
            return
        self._initialized = True

        # Базовые настройки
        base = api_base_url or getattr(settings, "piratebay_api_base", "https://apibay.org")
        self.api_base = base.rstrip("/")

        # HTTP сессия с retry и pooling
        self.http = RetryableSession(timeout=20, retries=3)

        # Redis с connection pooling
        r_url = redis_url or getattr(settings, "redis_url", "redis://localhost:6379/0")
        self.redis_pool = ConnectionPool.from_url(
            r_url,
            max_connections=50,
            encoding="utf-8",
            decode_responses=True
        )
        self.redis: Redis = Redis(connection_pool=self.redis_pool)

        # TTL настройки
        self.search_ttl = int(getattr(settings, "piratebay_search_ttl", 5 * 60))
        self.filelist_ttl = int(getattr(settings, "piratebay_filelist_ttl", 24 * 3600))
        self.torrent_ttl = int(getattr(settings, "piratebay_torrent_ttl", 7 * 24 * 3600))
        self.id2hash_ttl = int(getattr(settings, "piratebay_id2hash_ttl", 24 * 3600))
        self.empty_fl_ttl = int(getattr(settings, "piratebay_empty_filelist_ttl", 60 * 60))

        # Limiter для параллельных операций
        max_concurrent = int(max_filelist_concurrency or
                             getattr(settings, "piratebay_filelist_concurrency", 20))
        self.limiter = ConcurrentLimiter(max_concurrent)

        # Настройки для filelist
        self._fl_topn = int(getattr(settings, "piratebay_filelist_topn", 20))
        self._mirror_per_timeout = float(getattr(settings, "piratebay_mirror_timeout_per", 2.5))
        self._mirror_overall_timeout = float(getattr(settings, "piratebay_mirror_timeout_overall", 6.0))

        # Оптимизированные зеркала (отсортированы по надежности)
        self._mirrors = [
            "https://itorrents.org/torrent/{HEX}.torrent",
            "https://btcache.me/torrent/{HEX}.torrent",
            "https://torrage.info/torrent/{HEX}.torrent",
            "https://itorrents.org/download/{HEX}.torrent",
            "https://btcache.me/torrent/{HEX}",
        ]

        # HTML зеркала для парсинга
        mirrors = getattr(settings, "piratebay_html_mirrors", None)
        if isinstance(mirrors, (list, tuple)) and mirrors:
            self._html_mirrors: List[str] = [m.rstrip("/") for m in mirrors]
        else:
            self._html_mirrors = [
                "https://thepiratebay.org",
                "https://tpb.party",
                "https://pirateproxy.live",
                "https://thehiddenbay.com",
            ]

        # Трекеры по умолчанию (оптимизированный список)
        cfg_trackers = getattr(settings, "piratebay_default_trackers", None)
        self._default_trackers: List[str] = (
            list(cfg_trackers) if isinstance(cfg_trackers, (list, tuple)) and cfg_trackers else [
                "udp://tracker.opentrackr.org:1337/announce",
                "udp://open.stealth.si:80/announce",
                "udp://tracker.torrent.eu.org:451/announce",
                "udp://opentracker.i2p.rocks:6969/announce",
                "udp://explodie.org:6969/announce",
                "udp://tracker.internetwarriors.net:1337/announce",
                "http://tracker.opentrackr.org:1337/announce",
            ]
        )

        # DHT настройки
        self._dht_timeout_sec = int(
            getattr(settings, "piratebay_dht_timeout_sec", dht_timeout_sec or 90)
        )

        # Кеш для in-memory операций
        self._memory_cache: Dict[str, Tuple[Any, float]] = {}
        self._cache_lock = asyncio.Lock()

        log.info(
            "PirateBayService initialized: api=%s, redis=%s, DHT=%s, "
            "mirrors=%d, trackers=%d, dht_timeout=%ds",
            self.api_base, r_url, ("enabled" if lt else "disabled"),
            len(self._mirrors), len(self._default_trackers), self._dht_timeout_sec
        )

    # ----------------- Memory cache helpers -----------------

    async def _memory_get(self, key: str, ttl: int = 60) -> Optional[Any]:
        """Получить из in-memory кеша."""
        async with self._cache_lock:
            if key in self._memory_cache:
                value, timestamp = self._memory_cache[key]
                if time.time() - timestamp < ttl:
                    return value
                else:
                    del self._memory_cache[key]
        return None

    async def _memory_set(self, key: str, value: Any):
        """Сохранить в in-memory кеш."""
        async with self._cache_lock:
            # Ограничиваем размер кеша
            if len(self._memory_cache) > 1000:
                # Удаляем старые записи
                now = time.time()
                self._memory_cache = {
                    k: (v, t) for k, (v, t) in self._memory_cache.items()
                    if now - t < 3600  # Храним максимум час
                }
            self._memory_cache[key] = (value, time.time())

    # ----------------- Redis cache helpers (оптимизированные) -----------------

    async def _cache_get_blob(self, key: str) -> Optional[bytes]:
        """Получить blob из Redis с декодированием."""
        try:
            s = await self.redis.get(key)
            if not s:
                return None
            return base64.b64decode(s)
        except Exception as e:
            log.error(f"Failed to get blob from cache {key}: {e}")
            return None

    async def _cache_set_blob(self, key: str, data: bytes, ex: int) -> None:
        """Сохранить blob в Redis с кодированием."""
        try:
            b64 = base64.b64encode(data).decode("ascii")
            await self.redis.set(key, b64, ex=ex)
        except Exception as e:
            log.error(f"Failed to set blob to cache {key}: {e}")

    async def _cache_get_json(self, key: str) -> Optional[Any]:
        """Получить JSON из кеша."""
        try:
            data = await self.redis.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            log.error(f"Failed to get json from cache {key}: {e}")
        return None

    async def _cache_set_json(self, key: str, value: Any, ex: int) -> None:
        """Сохранить JSON в кеш."""
        try:
            await self.redis.set(key, json.dumps(value), ex=ex)
        except Exception as e:
            log.error(f"Failed to set json to cache {key}: {e}")

    # ------------------------------ Search (оптимизированный) ------------------------------

    async def search(
            self,
            query: str,
            only_lossless: Optional[bool] = None,
            track: Optional[str] = None,
    ) -> List[TorrentInfo]:
        """Поиск торрентов с оптимизацией."""

        # Проверяем memory cache сначала
        mem_key = f"search:{query}:{only_lossless}:{track}"
        if mem_result := await self._memory_get(mem_key, ttl=60):
            return mem_result

        # Redis cache
        cache_key = f"pb:search:{query}:{only_lossless}:{track}"
        if cached := await self._cache_get_json(cache_key):
            result = [TorrentInfo(**d) for d in cached]
            await self._memory_set(mem_key, result)
            return result

        # API запрос с retry
        url = f"{self.api_base}/q.php"
        params = {"q": query, "cat": 100}  # Audio category

        try:
            resp = await self.http.get(url, params=params)
            items = await resp.json()
        except Exception as e:
            log.error(f"Search API error: {e}")
            # Fallback на альтернативный endpoint если есть
            try:
                alt_url = f"{self.api_base}/search.php"
                resp = await self.http.get(alt_url, params=params)
                items = await resp.json()
            except Exception:
                raise HTTPException(status_code=502, detail="PirateBay API error")

        # Фильтрация и обработка результатов
        clean_items = []
        for it in items if isinstance(items, list) else []:
            if not isinstance(it, dict):
                continue
            tid = str(it.get("id", "")).strip()
            name = (it.get("name") or "").strip()

            # Пропускаем невалидные записи
            if not tid.isdigit() or tid == "0" or name.lower().startswith("no results"):
                continue

            # Базовая нормализация
            it["name"] = name
            it["seeders"] = max(0, int(it.get("seeders", 0) or 0))
            it["leechers"] = max(0, int(it.get("leechers", 0) or 0))
            it["size"] = max(0, int(it.get("size", 0) or 0))
            clean_items.append(it)

        # Lossless/lossy фильтрация
        filtered = []
        for it in clean_items:
            name = it.get("name", "")
            is_lossless = bool(_LOSSLESS_RE.search(name))
            is_lossy = bool(_LOSSY_RE.search(name))

            if only_lossless is True and (not is_lossless or is_lossy):
                continue
            if only_lossless is False and is_lossless:
                continue

            filtered.append(it)

        # Track фильтрация с оптимизацией
        if track:
            filtered = await self._filter_by_track(filtered, track)

        # Сборка результатов
        infos: List[TorrentInfo] = []
        out: List[dict] = []

        for it in filtered:
            size_bytes = it.get("size", 0)
            page_url = f"https://thepiratebay.org/?t={it['id']}"

            info = TorrentInfo(
                title=it.get("name", ""),
                url=page_url,
                size=_human_size(size_bytes),
                seeders=it.get("seeders", 0),
                leechers=it.get("leechers", 0),
            )
            infos.append(info)
            out.append(info.__dict__)

        # Кешируем результаты
        await self._cache_set_json(cache_key, out, ex=self.search_ttl)
        await self._memory_set(mem_key, infos)

        return infos

    async def _filter_by_track(self, items: List[dict], track: str) -> List[dict]:
        """Оптимизированная фильтрация по треку."""
        track_lower = track.lower()

        # Быстрый проход по заголовкам
        fast_hits = []
        candidates = []

        for it in items:
            name = it.get("name", "")
            if _title_maybe_contains_track(name, track):
                fast_hits.append(it)
            else:
                candidates.append(it)

        if not candidates:
            return fast_hits

        # Приоритизация кандидатов для глубокой проверки
        def _priority_score(item: dict) -> float:
            """Вычисляет приоритет для проверки."""
            score = 0.0

            # Seeders важнее всего
            score += item.get("seeders", 0) * 100

            # Размер тоже важен (большие сборники вероятнее содержат нужный трек)
            size_gb = item.get("size", 0) / (1024 ** 3)
            score += min(size_gb * 10, 100)

            # Ключевые слова в названии
            name_lower = item.get("name", "").lower()
            keywords = ["discog", "complete", "collection", "anthology", "best", "full"]
            for kw in keywords:
                if kw in name_lower:
                    score += 50

            return score

        # Сортируем по приоритету
        candidates.sort(key=_priority_score, reverse=True)

        # Ограничиваем количество для проверки
        max_check = min(len(candidates), self._fl_topn)
        to_check = candidates[:max_check]

        # Параллельная проверка с приоритетами
        async def check_item(item: dict, priority: int) -> Optional[dict]:
            tid = str(item["id"])

            async with self.limiter.acquire(priority):
                try:
                    fl = await self._get_filelist(tid)

                    # Проверяем файлы
                    for fname in fl:
                        f_lower = fname.lower()
                        if track_lower in f_lower or fuzz.partial_ratio(track_lower, f_lower) >= 80:
                            return item

                except Exception as e:
                    log.debug(f"Failed to check filelist for {tid}: {e}")

            return None

        # Запускаем проверки с приоритетами
        tasks = []
        for i, item in enumerate(to_check):
            priority = max_check - i  # Чем раньше в списке, тем выше приоритет
            tasks.append(check_item(item, priority))

        # Собираем результаты
        results = await asyncio.gather(*tasks, return_exceptions=True)
        deep_hits = [r for r in results if r and not isinstance(r, Exception)]

        return fast_hits + deep_hits

    # ------------------------------ Parse torrent helpers ------------------------------

    def _parse_torrent_filelist(self, data: bytes) -> List[str]:
        """Парсит список файлов из torrent blob."""
        try:
            meta = bencodepy.decode(data)
        except Exception:
            return []

        info = meta.get(b"info") if isinstance(meta, dict) else None
        if not isinstance(info, dict):
            return []

        # Multi-file torrent
        files = info.get(b"files")
        if isinstance(files, list) and files:
            out: List[str] = []
            for f in files:
                if not isinstance(f, dict):
                    continue
                parts = f.get(b"path")
                if isinstance(parts, list) and parts:
                    try:
                        name = "/".join([p.decode("utf-8", "ignore") for p in parts])
                        if name:
                            out.append(name)
                    except Exception:
                        continue
            return out

        # Single-file torrent
        name = info.get(b"name")
        if isinstance(name, (bytes, bytearray)):
            s = name.decode("utf-8", "ignore").strip()
            return [s] if s else []

        return []

    # ------------------------------ Mirrors fetching (оптимизированный) ------------------------------

    async def _fetch_from_mirrors(self, info_hash_hex: str, title_slug: str) -> Optional[bytes]:
        """Параллельный запрос к зеркалам с быстрым возвратом первого успешного."""

        async def fetch_one(url: str) -> Optional[bytes]:
            """Пытается загрузить с одного зеркала."""
            url = url.replace("{HEX}", info_hash_hex).replace("{SLUG}", title_slug)

            try:
                timeout = ClientTimeout(total=self._mirror_per_timeout)
                resp = await self.http.get(url, timeout=timeout, allow_redirects=True)
                data = await resp.read()

                # Проверяем, что это действительно torrent
                ct = (resp.headers.get("Content-Type") or "").lower()
                if resp.status == 200:
                    if ct.startswith("application/x-bittorrent") or data.startswith(b"d8:"):
                        return data

                    # Иногда возвращают HTML с редиректом
                    if ct.startswith("text/html"):
                        text = data.decode("utf-8", errors="ignore")
                        # Ищем прямую ссылку на torrent
                        m = re.search(r'href=["\'](https?://[^"\']+\.torrent[^"\']*)["\']',
                                      text, re.IGNORECASE)
                        if m:
                            real_url = m.group(1)
                            resp2 = await self.http.get(real_url, timeout=timeout,
                                                        allow_redirects=True)
                            data2 = await resp2.read()
                            if resp2.status == 200 and data2.startswith(b"d"):
                                return data2

            except asyncio.TimeoutError:
                log.debug(f"Timeout fetching from {url[:50]}...")
            except Exception as e:
                log.debug(f"Error fetching from {url[:50]}...: {e}")

            return None

        # Создаем задачи для всех зеркал
        tasks = [asyncio.create_task(fetch_one(mirror)) for mirror in self._mirrors]

        try:
            # Ждем первый успешный результат
            done, pending = await asyncio.wait(
                tasks,
                timeout=self._mirror_overall_timeout,
                return_when=asyncio.FIRST_COMPLETED
            )

            # Проверяем завершенные задачи
            for task in done:
                try:
                    result = task.result()
                    if result:
                        # Отменяем остальные задачи
                        for p in pending:
                            p.cancel()
                        return result
                except Exception:
                    continue

            # Если первая волна не дала результатов, ждем остальные
            if pending:
                done2, pending2 = await asyncio.wait(
                    pending,
                    timeout=max(1.0, self._mirror_overall_timeout / 2)
                )

                for task in done2:
                    try:
                        result = task.result()
                        if result:
                            for p in pending2:
                                p.cancel()
                            return result
                    except Exception:
                        continue

                # Отменяем оставшиеся
                for p in pending2:
                    p.cancel()

        finally:
            # Гарантированно отменяем все незавершенные задачи
            for task in tasks:
                if not task.done():
                    task.cancel()

        return None

    # ------------------------------ Get filelist (оптимизированный) ------------------------------

    async def _get_filelist(self, torrent_id: str) -> List[str]:
        """Получает список файлов для торрента."""

        # Memory cache
        mem_key = f"fl:{torrent_id}"
        if mem_fl := await self._memory_get(mem_key, ttl=300):
            return mem_fl

        # Redis cache
        cache_key = f"pb:fl:{torrent_id}"
        if cached := await self._cache_get_json(cache_key):
            await self._memory_set(mem_key, cached)
            return cached

        # Проверяем, не пустой ли это торрент
        if await self.redis.get(f"pb:fl_empty:{torrent_id}"):
            return []

        # Пробуем API
        names = await self._fetch_filelist_from_api(torrent_id)
        if names:
            await self._cache_set_json(cache_key, names, ex=self.filelist_ttl)
            await self._memory_set(mem_key, names)
            return names

        # Пробуем через mirrors
        info_hash_hex = await self._resolve_info_hash(torrent_id)
        if not info_hash_hex:
            await self.redis.set(f"pb:fl_empty:{torrent_id}", "1", ex=self.empty_fl_ttl)
            return []

        # Получаем torrent blob
        blob = await self._get_torrent_blob(info_hash_hex)
        if not blob:
            await self.redis.set(f"pb:fl_empty:{torrent_id}", "1", ex=self.empty_fl_ttl)
            return []

        # Парсим файлы
        fl = self._parse_torrent_filelist(blob)
        if fl:
            await self._cache_set_json(cache_key, fl, ex=self.filelist_ttl)
            await self._memory_set(mem_key, fl)
        else:
            await self.redis.set(f"pb:fl_empty:{torrent_id}", "1", ex=self.empty_fl_ttl)

        return fl

    async def _fetch_filelist_from_api(self, torrent_id: str) -> List[str]:
        """Получает список файлов через API."""
        url = f"{self.api_base}/f.php"
        names: List[str] = []

        try:
            resp = await self.http.get(url, params={"id": torrent_id})

            # Пробуем разные способы парсинга ответа
            try:
                files = await resp.json(content_type=None)
            except ContentTypeError:
                text = await resp.text()
                files = json.loads(text) if text else []

            # Извлекаем имена файлов
            for f in files if isinstance(files, list) else []:
                if isinstance(f, dict):
                    nl = f.get("name")
                    if isinstance(nl, list) and nl:
                        names.append(nl[0])
                    elif isinstance(nl, str) and nl:
                        names.append(nl)

        except Exception as e:
            log.debug(f"Failed to fetch filelist from API for {torrent_id}: {e}")

        return names

    async def _get_torrent_blob(self, info_hash_hex: str) -> Optional[bytes]:
        """Получает blob торрента по hash."""
        # Cache check
        blob_key = f"pb:blob:{info_hash_hex}"
        if cached := await self._cache_get_blob(blob_key):
            return cached

        # Fetch from mirrors
        title_slug = "torrent"  # Default slug
        blob = await self._fetch_from_mirrors(info_hash_hex, title_slug)

        if blob:
            await self._cache_set_blob(blob_key, blob, ex=self.torrent_ttl)

        return blob

    # ------------------------------ Resolve info hash ------------------------------

    async def _resolve_info_hash(self, torrent_id: str) -> Optional[str]:
        """Резолвит info hash по ID торрента."""

        # Memory cache
        mem_key = f"id2hash:{torrent_id}"
        if mem_hash := await self._memory_get(mem_key, ttl=3600):
            return mem_hash

        # Redis cache
        cache_key = f"pb:id2hash:{torrent_id}"
        if cached := await self.redis.get(cache_key):
            ih = (cached or "").strip()
            if ih:
                await self._memory_set(mem_key, ih)
                return ih

        # Try API first
        url = f"{self.api_base}/t.php"
        try:
            resp = await self.http.get(url, params={"id": torrent_id})
            meta = await resp.json()

            ih_raw = meta.get("info_hash") or meta.get("infohash") or ""
            ih_norm = _hex_upper_from_btih(ih_raw)

            if ih_norm:
                await self.redis.set(cache_key, ih_norm, ex=self.id2hash_ttl)
                await self._memory_set(mem_key, ih_norm)
                return ih_norm

        except Exception as e:
            log.debug(f"Failed to resolve hash from API for {torrent_id}: {e}")

        # Try HTML parsing as fallback
        ih_fb = await self._extract_hash_from_html(torrent_id)
        if ih_fb:
            await self.redis.set(cache_key, ih_fb, ex=self.id2hash_ttl)
            await self._memory_set(mem_key, ih_fb)
            return ih_fb

        return None

    async def _extract_hash_from_html(self, torrent_id: str) -> Optional[str]:
        """Извлекает hash из HTML страницы."""

        # Пробуем разные URL паттерны
        paths = [
            f"/description.php?id={torrent_id}",
            f"/?t={torrent_id}",
            f"/torrent/{torrent_id}",
        ]

        for mirror in self._html_mirrors[:2]:  # Пробуем только первые 2 зеркала
            for path in paths:
                url = f"{mirror}{path}"

                try:
                    timeout = ClientTimeout(total=5)
                    resp = await self.http.get(url, timeout=timeout)

                    if resp.status != 200:
                        continue

                    html = await resp.text()

                    # Ищем magnet ссылку
                    m = re.search(r'href=["\'](magnet:\?[^"\']+)', html, re.IGNORECASE)
                    if not m:
                        continue

                    magnet = m.group(1)
                    parsed = urllib.parse.urlparse(magnet)
                    qs = urllib.parse.parse_qs(parsed.query)

                    # Извлекаем BTIH
                    for xt in qs.get("xt", []):
                        if xt.lower().startswith("urn:btih:"):
                            btih = _hex_upper_from_btih(xt)
                            if btih:
                                return btih

                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    log.debug(f"Failed to extract hash from {url}: {e}")
                    continue

        return None

    # ------------------------------ Harvest trackers ------------------------------

    async def _harvest_trackers(self, info_hash_hex: str) -> List[str]:
        """Собирает трекеры из разных источников."""
        trackers: List[str] = []
        seen: Set[str] = set()

        # URLs для сбора трекеров
        urls = [
            f"https://btcache.me/torrent/{info_hash_hex}",
            f"https://itorrents.org/torrent/{info_hash_hex}.torrent",
        ]

        async def fetch_trackers(url: str) -> List[str]:
            """Извлекает трекеры из HTML страницы."""
            result = []

            try:
                timeout = ClientTimeout(total=3)
                resp = await self.http.get(url, timeout=timeout, allow_redirects=True)

                if resp.status != 200:
                    return result

                ct = (resp.headers.get("Content-Type") or "").lower()
                if "text/html" in ct or "text/plain" in ct:
                    text = await resp.text(errors="ignore")

                    # Ищем magnet ссылки
                    for m in re.finditer(r'href=["\'](magnet:\?[^"\']+)["\']', text, re.IGNORECASE):
                        magnet = m.group(1)
                        try:
                            parsed = urllib.parse.urlparse(magnet)
                            qs = urllib.parse.parse_qs(parsed.query)

                            for tr in qs.get("tr", []):
                                tr_dec = urllib.parse.unquote_plus(tr).strip()
                                if tr_dec and tr_dec not in seen:
                                    result.append(tr_dec)
                                    seen.add(tr_dec)

                        except Exception:
                            continue

            except Exception:
                pass

            return result

        # Параллельный сбор трекеров
        tasks = [fetch_trackers(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, list):
                trackers.extend(result)

        return trackers

    # ------------------------------ DHT fallback ------------------------------

    def _setup_libtorrent_session(self, ses):
        """Настройка libtorrent сессии для оптимальной производительности."""
        if not lt:
            return

        try:
            sp = lt.settings_pack()

            # Настройки для быстрого получения метаданных
            settings_map = {
                "alert_mask": lt.alert.category_t.all_categories,
                "announce_to_all_trackers": True,
                "announce_to_all_tiers": True,
                "enable_outgoing_utp": True,
                "enable_incoming_utp": True,
                "enable_outgoing_tcp": True,
                "enable_incoming_tcp": True,
                "enable_dht": True,
                "enable_lsd": True,
                "enable_natpmp": True,
                "enable_upnp": True,
                "dht_bootstrap_nodes": "router.bittorrent.com:6881,dht.transmissionbt.com:6881",
                "max_metadata_size": 10 * 1024 * 1024,  # 10 MB
                "metadata_transfer_protocol": 2,  # ut_metadata
            }

            for key, value in settings_map.items():
                attr = getattr(lt.settings_pack, key, None)
                if attr is not None:
                    if isinstance(value, bool):
                        sp.set_bool(attr, value)
                    elif isinstance(value, int):
                        sp.set_int(attr, value)
                    elif isinstance(value, str):
                        sp.set_str(attr, value)

            # Интерфейсы для прослушивания
            try:
                sp.set_str(lt.settings_pack.listen_interfaces, "0.0.0.0:6881,[::]:6881")
            except Exception:
                pass

            ses.apply_settings(sp)

        except Exception as e:
            log.error(f"Failed to setup libtorrent session: {e}")

    def _dht_fetch_torrent_sync(self, info_hash_hex: str, timeout_sec: int,
                                trackers: List[str]) -> Optional[bytes]:
        """Синхронное получение торрента через DHT."""
        if not lt:
            return None

        ses = lt.session()

        try:
            # Настройка сессии
            self._setup_libtorrent_session(ses)

            # Порты для прослушивания
            try:
                ses.listen_on(6881, 6891)
            except Exception:
                pass

            # DHT роутеры
            for host, port in [
                ("router.bittorrent.com", 6881),
                ("router.utorrent.com", 6881),
                ("dht.transmissionbt.com", 6881),
                ("dht.aelitis.com", 6881),
            ]:
                try:
                    ses.add_dht_router(host, port)
                except Exception:
                    pass

            # Запуск DHT
            try:
                ses.start_dht()
            except Exception:
                pass

            # Создание magnet URI
            uri = f"magnet:?xt=urn:btih:{info_hash_hex}"
            params = lt.parse_magnet_uri(uri)

            # Настройка параметров
            try:
                params.save_path = "."
            except Exception:
                try:
                    params["save_path"] = "."
                except Exception:
                    pass

            # Добавление трекеров
            all_trackers = list(set(trackers + self._default_trackers))

            try:
                if hasattr(params, "trackers"):
                    params.trackers.extend(all_trackers)
                elif isinstance(params, dict):
                    params.setdefault("trackers", [])
                    params["trackers"].extend(all_trackers)
            except Exception:
                pass

            # Добавление торрента
            h = ses.add_torrent(params)

            # Добавление трекеров к handle
            for tr in all_trackers:
                try:
                    h.add_tracker(tr)
                except Exception:
                    pass

            # Форсируем анонсы
            for _ in range(2):
                try:
                    h.force_reannounce()
                except Exception:
                    pass
                try:
                    h.force_dht_announce()
                except Exception:
                    pass

            # Ждем метаданные
            start = time.time()
            while time.time() - start < timeout_sec:
                if h.has_metadata():
                    break
                time.sleep(0.1)

            if not h.has_metadata():
                return None

            # Получаем torrent info
            ti = h.get_torrent_info()

            # Создаем torrent
            try:
                ct = lt.create_torrent(ti)
            except Exception:
                try:
                    ct = ti.create_torrent()
                except Exception:
                    ct = lt.create_torrent(ti)

            # Добавляем трекеры в torrent
            for tr in all_trackers:
                try:
                    ct.add_tracker(tr)
                except Exception:
                    pass

            # Генерируем данные
            try:
                torrent_dict = ct.generate()
                data = lt.bencode(torrent_dict)
            except Exception:
                try:
                    data = lt.bencode(ct.generate())
                except Exception:
                    return None

            return bytes(data)

        except Exception as e:
            log.error(f"DHT fetch failed: {e}")
            return None

        finally:
            # Cleanup
            try:
                ses.pause()
                ses.stop_dht()
            except Exception:
                pass

    async def _dht_fetch_torrent(self, info_hash_hex: str, timeout_sec: int,
                                 trackers: List[str]) -> Optional[bytes]:
        """Асинхронная обертка для DHT fetch."""
        return await asyncio.to_thread(
            self._dht_fetch_torrent_sync,
            info_hash_hex,
            timeout_sec,
            trackers
        )

    # ------------------------------ Download ------------------------------

    async def download(self, info: TorrentInfo) -> bytes:
        """Скачивает torrent файл."""

        # Извлекаем info hash из URL
        info_hash_hex = None
        trackers: List[str] = []

        if info.url.startswith("magnet:"):
            # Magnet link
            parsed = urlparse(info.url)
            qs = parse_qs(parsed.query)

            # Извлекаем трекеры
            for tr in qs.get("tr", []):
                trackers.append(urllib.parse.unquote_plus(tr))

            # Извлекаем hash
            for xt in qs.get("xt", []):
                if xt.lower().startswith("urn:btih:"):
                    info_hash_hex = _hex_upper_from_btih(xt)
                    break

        else:
            # Regular URL - extract torrent ID
            parsed = urlparse(info.url)
            qs = parse_qs(parsed.query)

            tid = None
            for key in ("t", "id"):
                if key in qs and qs[key]:
                    tid = qs[key][0]
                    break

            if tid:
                # Резолвим hash
                info_hash_hex = await self._resolve_info_hash(tid)

                # Собираем трекеры из HTML
                if not trackers and info_hash_hex:
                    trackers = await self._harvest_trackers(info_hash_hex)

        if not info_hash_hex:
            raise HTTPException(
                status_code=422,
                detail="Cannot resolve info hash. Torrent may be v2 (btmh) or missing."
            )

        # Проверяем кеш
        cache_key = f"pb:blob:{info_hash_hex}"
        if cached := await self._cache_get_blob(cache_key):
            return cached

        # Пробуем mirrors
        title_slug = _slug(info.title) if info.title else "torrent"
        data = await self._fetch_from_mirrors(info_hash_hex, title_slug)

        if data:
            await self._cache_set_blob(cache_key, data, ex=self.torrent_ttl)
            return data

        # Собираем все трекеры
        all_trackers = list(set(trackers + self._default_trackers))

        # DHT fallback
        if lt:
            log.info(f"Trying DHT fetch for {info_hash_hex} with {len(all_trackers)} trackers")
            dht_data = await self._dht_fetch_torrent(
                info_hash_hex,
                self._dht_timeout_sec,
                all_trackers
            )

            if dht_data:
                await self._cache_set_blob(cache_key, dht_data, ex=self.torrent_ttl)
                return dht_data

        # Не удалось получить
        detail = "Torrent not found in public caches"
        if not lt:
            detail += "; DHT fallback unavailable (libtorrent not installed)"

        raise HTTPException(status_code=404, detail=detail)

    async def download_by_id(self, torrent_id: str) -> bytes:
        """Скачивает torrent по ID."""
        ih = await self._resolve_info_hash(torrent_id)
        if not ih:
            raise HTTPException(status_code=404, detail="Metadata not found or unsupported")

        ti = TorrentInfo(
            title="",
            url=f"https://thepiratebay.org/?t={torrent_id}",
            size="0 B",
            seeders=0,
            leechers=0
        )
        return await self.download(ti)

    async def download_by_hash(self, info_hash: str) -> bytes:
        """Скачивает torrent по hash."""
        ih = _hex_upper_from_btih(info_hash)
        if not ih:
            raise HTTPException(status_code=422, detail="Invalid BTIH format")

        ti = TorrentInfo(
            title="",
            url=f"magnet:?xt=urn:btih:{ih}",
            size="0 B",
            seeders=0,
            leechers=0
        )
        return await self.download(ti)

    # ------------------------------ Cleanup ------------------------------

    async def close(self) -> None:
        """Закрывает сервис и освобождает ресурсы."""
        try:
            await self.http.close()
        except Exception:
            pass

        try:
            await self.redis.aclose()
        except Exception:
            pass

        log.info("PirateBayService closed")

    def __del__(self):
        """Деструктор для очистки при удалении объекта."""
        try:
            if hasattr(self, 'http'):
                asyncio.create_task(self.http.close())
        except Exception:
            pass


# Singleton getter для FastAPI
@lru_cache()
def get_piratebay_service() -> PirateBayService:
    """Возвращает singleton экземпляр сервиса."""
    return PirateBayService()