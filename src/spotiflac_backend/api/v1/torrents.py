import io
import re
import urllib.parse
import asyncio
import logging
from typing import List, Optional
from enum import Enum
from contextlib import asynccontextmanager

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from spotiflac_backend.core.config import settings
from spotiflac_backend.services.rutracker import (
    RutrackerService, CaptchaRequired, get_rutracker_service
)
from spotiflac_backend.services.pirate_bay_service import (
    PirateBayService, get_piratebay_service
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="")

# HTTP коды
CAPTCHA_REQUIRED = 428


# ========================= Models =========================

class TorrentInfoResponse(BaseModel):
    """Модель ответа с информацией о торренте."""
    title: str
    url: str
    size: str
    seeders: int
    leechers: int
    source: Optional[str] = None  # источник (rutracker/piratebay)


class CaptchaInitResponse(BaseModel):
    """Модель ответа при инициации CAPTCHA."""
    session_id: str
    captcha_image: str


class CaptchaCompleteRequest(BaseModel):
    """Модель запроса для решения CAPTCHA."""
    session_id: str = Field(..., min_length=1, max_length=100)
    solution: str = Field(..., min_length=1, max_length=20)


class TorrentSource(str, Enum):
    """Источники торрентов."""
    RUTRACKER = "rutracker"
    PIRATEBAY = "piratebay"


# ========================= Services =========================

@asynccontextmanager
async def managed_services():
    """Контекстный менеджер (singleton-ы из фабрик)."""
    rt_svc = get_rutracker_service()
    pb_svc = get_piratebay_service()
    try:
        yield rt_svc, pb_svc
    finally:
        # singleton — не закрываем здесь
        pass


# ========================= Utils =========================

def is_info_hash(value: str) -> bool:
    """Проверяет, является ли строка info hash (40 hex символов)."""
    return bool(re.fullmatch(r"[0-9a-fA-F]{40}", value))


def normalize_topic_id(topic_id: str) -> str:
    """Нормализует topic_id для консистентности."""
    if is_info_hash(topic_id):
        return topic_id.upper()
    return topic_id.strip()


def extract_topic_id(url: str) -> Optional[str]:
    """Извлекает topic_id из URL."""
    try:
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        return params.get("t", [None])[0]
    except Exception:
        return None


def make_cache_key(source: TorrentSource, topic_id: str) -> str:
    """Создает ключ кеша для торрента (бинари)."""
    normalized_id = normalize_topic_id(topic_id)
    if source == TorrentSource.PIRATEBAY:
        if is_info_hash(normalized_id):
            return f"torrent:pb:hash:{normalized_id}"
        else:
            return f"torrent:pb:id:{normalized_id}"
    else:
        return f"torrent:rt:{normalized_id}"


def make_source_key(topic_id: str) -> str:
    """Ключ для хранения источника торрента (строки)."""
    return f"torrent:source:{normalize_topic_id(topic_id)}"


# ========================= Search =========================

async def search_rutracker_with_retry(
    rt_svc: RutrackerService,
    query: str,
    lossless: Optional[bool],
    track: Optional[str],
    max_retries: int = 3
) -> List[TorrentInfoResponse]:
    """Поиск в RuTracker с повторами."""
    for attempt in range(max_retries + 1):
        try:
            results = await rt_svc.search(query, only_lossless=lossless, track=track)
            if results:
                return [
                    TorrentInfoResponse(
                        title=r.title,
                        url=r.url,
                        size=r.size,
                        seeders=r.seeders,
                        leechers=r.leechers,
                        source=TorrentSource.RUTRACKER
                    )
                    for r in results
                ]
            if attempt < max_retries:
                log.debug(f"RuTracker empty (attempt {attempt + 1}/{max_retries + 1}), retrying...")
                await asyncio.sleep(0.5 * (attempt + 1))
        except CaptchaRequired:
            raise
        except Exception as e:
            if attempt == max_retries:
                log.error(f"RuTracker search failed after {max_retries + 1} attempts: {e}")
                raise
            log.warning(f"RuTracker error (attempt {attempt + 1}/{max_retries + 1}): {e}")
            await asyncio.sleep(0.5 * (attempt + 1))
    return []


async def search_piratebay_safe(
    pb_svc: PirateBayService,
    query: str,
    lossless: Optional[bool],
    track: Optional[str]
) -> List[TorrentInfoResponse]:
    """Поиск в PirateBay с обработкой ошибок."""
    try:
        results = await pb_svc.search(query, only_lossless=lossless, track=track)
        return [
            TorrentInfoResponse(
                title=r.title,
                url=r.url,
                size=r.size,
                seeders=r.seeders,
                leechers=r.leechers,
                source=TorrentSource.PIRATEBAY
            )
            for r in results
        ]
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"PirateBay search error: {e}")
        return []


async def cache_search_results_text(
    redis_text,
    results: List[TorrentInfoResponse],
    ttl: int = 24 * 3600
) -> None:
    """
    Кешируем только **источник** (строка) для найденных topic_id.
    Используем текстовый клиент (decode_responses=True).
    """
    if not results:
        return

    pipe = redis_text.pipeline()
    for r in results:
        topic_id = extract_topic_id(r.url)
        if topic_id:
            source_key = make_source_key(topic_id)
            source_value = (r.source or TorrentSource.PIRATEBAY)
            pipe.setex(source_key, ttl, source_value)
    try:
        await pipe.execute()
    except Exception as e:
        log.warning(f"Failed to cache search results: {e}")


# ========================= Download & Cache =========================

async def download_from_piratebay(
    pb_svc: PirateBayService,
    topic_id: str,
    redis_text,
    strict: bool = False
) -> Optional[bytes]:
    """Скачивание с PirateBay."""
    try:
        if is_info_hash(topic_id):
            data = await pb_svc.download_by_hash(topic_id)
        else:
            data = await pb_svc.download_by_id(topic_id)

        # Сохраняем источник (строка)
        source_key = make_source_key(topic_id)
        await redis_text.setex(source_key, 24 * 3600, TorrentSource.PIRATEBAY)
        return data

    except HTTPException as e:
        if strict:
            raise
        if e.status_code in (404, 422):
            return None
        raise
    except Exception as e:
        if strict:
            raise HTTPException(status_code=502, detail=f"PirateBay error: {str(e)}")
        log.warning(f"PirateBay download failed for {topic_id}: {e}")
        return None


async def download_from_rutracker(
    rt_svc: RutrackerService,
    topic_id: str,
    redis_text,
    strict: bool = False
) -> Optional[bytes]:
    """Скачивание с RuTracker."""
    if not topic_id.isdigit():
        if strict:
            raise HTTPException(status_code=400, detail="Invalid RuTracker ID format")
        return None

    try:
        data = await rt_svc.download(int(topic_id))
        source_key = make_source_key(topic_id)
        await redis_text.setex(source_key, 24 * 3600, TorrentSource.RUTRACKER)
        return data

    except CaptchaRequired as c:
        raise HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": c.session_id, "captcha_image": c.img_url}
        )
    except Exception as e:
        if strict:
            raise HTTPException(status_code=502, detail=f"RuTracker error: {str(e)}")
        log.warning(f"RuTracker download failed for {topic_id}: {e}")
        return None


async def get_cached_torrent_bytes(
    redis_bytes,
    source: Optional[TorrentSource],
    topic_id: str
) -> Optional[bytes]:
    """Читает бинарный .torrent из кеша (bytes-клиент)."""
    if not source:
        return None
    cache_key = make_cache_key(source, topic_id)
    try:
        return await redis_bytes.get(cache_key)
    except Exception as e:
        log.warning(f"Cache read failed for {cache_key}: {e}")
        return None


async def cache_torrent_bytes(
    redis_bytes,
    source: TorrentSource,
    topic_id: str,
    data: bytes,
    ttl: int = 7 * 24 * 3600
) -> None:
    """Кладёт бинарный .torrent в кеш (bytes-клиент)."""
    cache_key = make_cache_key(source, topic_id)
    try:
        await redis_bytes.setex(cache_key, ttl, data)
    except Exception as e:
        log.warning(f"Cache write failed for {cache_key}: {e}")


async def detect_source_text(redis_text, topic_id: str) -> Optional[TorrentSource]:
    """Определяет источник торрента (строка в Redis)."""
    source_key = make_source_key(topic_id)
    try:
        value = await redis_text.get(source_key)
        if value:
            source_str = str(value).lower()
            if source_str == TorrentSource.RUTRACKER:
                return TorrentSource.RUTRACKER
            if source_str == TorrentSource.PIRATEBAY:
                return TorrentSource.PIRATEBAY
    except Exception as e:
        log.warning(f"Failed to detect source for {topic_id}: {e}")
    return None


# ========================= Endpoints =========================

@router.post(
    "/login/initiate",
    response_model=CaptchaInitResponse,
    responses={CAPTCHA_REQUIRED: {"model": CaptchaInitResponse}},
    summary="Инициировать процесс логина с CAPTCHA"
)
async def login_initiate(request: Request):
    rt_svc = get_rutracker_service()
    try:
        rt_svc.initiate_login()
        return {"session_id": "", "captcha_image": ""}
    except CaptchaRequired as c:
        raise HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": c.session_id, "captcha_image": c.img_url}
        )


@router.post("/login/complete", summary="Завершить логин с решением CAPTCHA")
async def login_complete(body: CaptchaCompleteRequest):
    rt_svc = get_rutracker_service()
    try:
        rt_svc.complete_login(body.session_id, body.solution)
        return {"status": "ok", "message": "Login successful"}
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.error(f"Login completion error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/search",
    response_model=List[TorrentInfoResponse],
    responses={CAPTCHA_REQUIRED: {"description": "Captcha required"}},
    summary="Поиск торрентов"
)
async def search_torrents(
    request: Request,
    q: str = Query(..., title="Search query", min_length=1, max_length=200),
    lossless: Optional[bool] = Query(None, title="Only lossless"),
    track: Optional[str] = Query(None, title="Track name", max_length=100),
):
    """
    Параллельный поиск в RuTracker и PirateBay с объединением результатов.
    """
    # Берём готовые клиенты из app.state
    redis_bytes = request.app.state.redis_client   # decode_responses=False
    redis_text  = request.app.state.cache_client   # decode_responses=True

    async with managed_services() as (rt_svc, pb_svc):
        max_rt_retries = int(getattr(settings, "rutracker_search_retries", 3))

        rt_task = asyncio.create_task(
            search_rutracker_with_retry(rt_svc, q, lossless, track, max_rt_retries)
        )
        pb_task = asyncio.create_task(
            search_piratebay_safe(pb_svc, q, lossless, track)
        )

        try:
            rt_results, pb_results = await asyncio.gather(rt_task, pb_task, return_exceptions=True)
        except Exception as e:
            log.error(f"Search gather error: {e}")
            rt_results, pb_results = [], []

        if isinstance(rt_results, CaptchaRequired):
            raise HTTPException(
                status_code=CAPTCHA_REQUIRED,
                detail={"session_id": rt_results.session_id, "captcha_image": rt_results.img_url}
            )
        elif isinstance(rt_results, Exception):
            log.error(f"RuTracker search exception: {rt_results}")
            rt_results = []

        if isinstance(pb_results, Exception):
            log.error(f"PirateBay search exception: {pb_results}")
            pb_results = []

        all_results = (rt_results or []) + (pb_results or [])

        # Кешируем источники (только текстовый клиент)
        if all_results:
            await cache_search_results_text(redis_text, all_results)

        # Сортируем по сидерам
        all_results.sort(key=lambda x: x.seeders, reverse=True)
        return all_results


@router.get(
    "/search/piratebay",
    response_model=List[TorrentInfoResponse],
    summary="Поиск только в PirateBay"
)
async def search_piratebay(
    request: Request,
    q: str = Query(..., title="Search query", min_length=1, max_length=200),
    lossless: Optional[bool] = Query(None, title="Only lossless"),
    track: Optional[str] = Query(None, title="Track name", max_length=100),
):
    redis_text = request.app.state.cache_client
    pb_svc = get_piratebay_service()
    try:
        results = await search_piratebay_safe(pb_svc, q, lossless, track)
        if results:
            await cache_search_results_text(redis_text, results)
        return results
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"PirateBay search error: {e}")
        raise HTTPException(status_code=502, detail=str(e))


@router.get(
    "/download/{topic_id}",
    response_class=StreamingResponse,
    responses={
        CAPTCHA_REQUIRED: {"description": "Captcha required"},
        404: {"description": "Torrent not found"},
        502: {"description": "Tracker error"}
    },
    summary="Скачать торрент файл"
)
async def download_torrent(
    request: Request,
    topic_id: str,
    tracker: Optional[TorrentSource] = Query(
        None,
        description="Source tracker. If omitted, auto-detected."
    ),
):
    """
    Скачивает .torrent файл по ID или info hash.
    """
    redis_bytes = request.app.state.redis_client
    redis_text  = request.app.state.cache_client

    topic_id = normalize_topic_id(topic_id)

    if tracker:
        source = tracker
    else:
        source = await detect_source_text(redis_text, topic_id)

    # Проверяем кеш (бинари)
    if source:
        cached = await get_cached_torrent_bytes(redis_bytes, source, topic_id)
        if cached:
            log.debug(f"Cache hit for {topic_id} from {source}")
            return StreamingResponse(
                io.BytesIO(cached),
                media_type="application/x-bittorrent",
                headers={
                    "Content-Disposition": f'attachment; filename="{topic_id}.torrent"',
                    "Cache-Control": "public, max-age=3600"
                }
            )

    async with managed_services() as (rt_svc, pb_svc):
        data: Optional[bytes] = None
        actual_source: Optional[TorrentSource] = None

        if source == TorrentSource.RUTRACKER:
            data = await download_from_rutracker(rt_svc, topic_id, redis_text, strict=True)
            actual_source = TorrentSource.RUTRACKER
        elif source == TorrentSource.PIRATEBAY:
            data = await download_from_piratebay(pb_svc, topic_id, redis_text, strict=True)
            actual_source = TorrentSource.PIRATEBAY
        else:
            data = await download_from_piratebay(pb_svc, topic_id, redis_text, strict=False)
            if data:
                actual_source = TorrentSource.PIRATEBAY
            else:
                data = await download_from_rutracker(rt_svc, topic_id, redis_text, strict=False)
                if data:
                    actual_source = TorrentSource.RUTRACKER

        if not data:
            raise HTTPException(status_code=404, detail="Torrent not found")

        # Кешируем бинарник
        if actual_source:
            await cache_torrent_bytes(redis_bytes, actual_source, topic_id, data)

        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/x-bittorrent",
            headers={
                "Content-Disposition": f'attachment; filename="{topic_id}.torrent"',
                "Cache-Control": "public, max-age=3600"
            }
        )