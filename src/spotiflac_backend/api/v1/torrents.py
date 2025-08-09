import io
import re
import urllib.parse
import asyncio
import logging
from typing import List, Optional

import aioredis
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from spotiflac_backend.core.config import settings
from spotiflac_backend.services.rutracker import RutrackerService, CaptchaRequired
from spotiflac_backend.services.pirate_bay_service import PirateBayService
log = logging.getLogger(__name__)
router = APIRouter(prefix="")  # без префикса

CAPTCHA_REQUIRED = 428


class TorrentInfoResponse(BaseModel):
    title: str
    url: str
    size: str
    seeders: int
    leechers: int


class CaptchaInitResponse(BaseModel):
    session_id: str
    captcha_image: str


class CaptchaCompleteRequest(BaseModel):
    session_id: str
    solution: str


# Initialise asynchronous Redis client.
redis: aioredis.Redis = aioredis.from_url(
    settings.redis_url, encoding="utf-8", decode_responses=False
)


@router.post(
    "/login/initiate",
    response_model=CaptchaInitResponse,
    responses={CAPTCHA_REQUIRED: {"model": CaptchaInitResponse}},
)
async def login_initiate(request: Request):
    svc = RutrackerService()
    try:
        sid, img_url = svc.initiate_login()
        if sid is None:
            return {"session_id": "", "captcha_image": ""}
        # shouldn't reach here, since initiate_login raises
        raise HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": sid, "captcha_image": img_url},
        )
    except CaptchaRequired as c:
        raise HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": c.session_id, "captcha_image": c.img_url},
        )
    finally:
        await svc.close()


@router.post("/login/complete")
async def login_complete(body: CaptchaCompleteRequest):
    svc = RutrackerService()
    try:
        svc.complete_login(body.session_id, body.solution)
        return {"status": "ok"}
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        await svc.close()


@router.get(
    "/search",
    response_model=List[TorrentInfoResponse],
    responses={CAPTCHA_REQUIRED: {"description": "Captcha required"}},
)
async def search_torrents(
    request: Request,
    q: str = Query(..., title="Search query"),
    lossless: Optional[bool] = Query(None, title="Only lossless"),
    track: Optional[str] = Query(None, title="Track name"),
):
    """Search both RuTracker and Pirate Bay in parallel and merge results."""
    rt_svc = RutrackerService()
    pb_svc = PirateBayService()

    # Запускаем PirateBay-поиск сразу
    pb_task = asyncio.create_task(
        pb_svc.search(q, only_lossless=lossless, track=track)
    )

    # Параметры повторных попыток
    max_rt_retries = getattr(settings, "rutracker_search_retries", 3)
    rt_results: List[TorrentInfoResponse] = []

    # 1) RuTracker с retry
    try:
        for attempt in range(1, max_rt_retries + 2):  # +1 для первой попытки
            rt_results = await rt_svc.search(q, only_lossless=lossless, track=track)
            if rt_results:
                break
            log.debug(
                "RuTracker search вернул пусто (попытка %d/%d), повторяю...",
                attempt,
                max_rt_retries + 1,
            )
    except CaptchaRequired as c:
        # сразу отдаём капчу
        await rt_svc.close()
        await pb_svc.close()
        raise HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": c.session_id, "captcha_image": c.img_url},
        )
    except RuntimeError as e:
        # если не получилось и у PirateBay нет результатов — отваливаем
        pb_res = []
        try:
            pb_res = await pb_task
        except Exception:
            pass
        await rt_svc.close()
        await pb_svc.close()
        if not pb_res:
            raise HTTPException(status_code=502, detail=str(e))
        # иначе просто игнорируем ошибку RuTracker и продолжаем с PirateBay
        rt_results = []

    # 2) PirateBay
    pb_results: List[TorrentInfoResponse] = []
    try:
        pb_results = await pb_task
    except HTTPException as e:
        if not rt_results:
            # если RuTracker ничего не вернул — отдадим ошибку PirateBay
            await rt_svc.close()
            await pb_svc.close()
            raise e
    except Exception as e:
        if not rt_results:
            await rt_svc.close()
            await pb_svc.close()
            raise HTTPException(status_code=502, detail=str(e))

    # Закрываем сессии
    await rt_svc.close()
    await pb_svc.close()

    # Кэшируем mapping для download
    ttl = 24 * 3600
    for r in rt_results:
        parsed = urllib.parse.urlparse(r.url)
        tid = urllib.parse.parse_qs(parsed.query).get("t")
        if tid:
            await redis.setex(f"torrent:source:{tid[0]}", ttl, b"rutracker")
    for r in pb_results:
        parsed = urllib.parse.urlparse(r.url)
        tid = urllib.parse.parse_qs(parsed.query).get("t")
        if tid:
            await redis.setex(f"torrent:source:{tid[0]}", ttl, b"piratebay")

    # Объединяем и возвращаем сразу оба списка
    return rt_results + pb_results



@router.get(
    "/search/piratebay",
    response_model=List[TorrentInfoResponse],
    responses={CAPTCHA_REQUIRED: {"description": "Captcha required"}},
)
async def search_piratebay(
    request: Request,
    q: str = Query(..., title="Search query"),
    lossless: Optional[bool] = Query(None, title="Only lossless"),
    track: Optional[str] = Query(None, title="Track name"),
):
    svc = PirateBayService()
    try:
        results = await svc.search(q, only_lossless=lossless, track=track)
        ttl = 24 * 3600
        for r in results:
            parsed = urllib.parse.urlparse(r.url)
            tid = urllib.parse.parse_qs(parsed.query).get("t")
            if tid:
                await redis.setex(f"torrent:source:{tid[0]}", ttl, b"piratebay")
        return results
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get(
    "/download/{topic_id}",
    response_class=StreamingResponse,
    responses={CAPTCHA_REQUIRED: {"description": "Captcha required"}},
)
async def download_torrent(
    request: Request,
    topic_id: str,
    tracker: Optional[str] = Query(
        None,
        description="Source tracker: 'rutracker' or 'piratebay'. If omitted, inferred automatically.",
    ),
):
    def is_hex_hash(s: str) -> bool:
        return len(s) == 40 and re.fullmatch(r"[0-9a-fA-F]{40}", s) is not None

    async def try_piratebay(_id: str, strict: bool) -> Optional[bytes]:
        svc = PirateBayService()
        try:
            if is_hex_hash(_id):
                data = await svc.download_by_hash(_id)
            else:
                data = await svc.download_by_id(_id)
            await redis.setex(f"torrent:source:{_id}", 24 * 3600, b"piratebay")
            return data
        except HTTPException as e:
            if strict:
                # источник однозначен — пробрасываем ошибку PB
                raise
            if e.status_code in (404, 422):
                return None
            raise
        finally:
            await svc.close()

    async def try_rutracker(_id: str, strict: bool) -> Optional[bytes]:
        if not _id.isdigit():
            return None if not strict else HTTPException(status_code=400, detail="Invalid RuTracker ID")
        svc = RutrackerService()
        try:
            data = await svc.download(int(_id))
            await redis.setex(f"torrent:source:{_id}", 24 * 3600, b"rutracker")
            return data
        except CaptchaRequired as c:
            raise HTTPException(
                status_code=CAPTCHA_REQUIRED,
                detail={"session_id": c.session_id, "captcha_image": c.img_url},
            )
        except Exception as e:
            if strict:
                # источник однозначен — пробрасываем 502
                raise HTTPException(status_code=502, detail=str(e))
            return None
        finally:
            await svc.close()

    # 1) Узнаём источник
    if tracker is None:
        try:
            src = await redis.get(f"torrent:source:{topic_id}")
        except Exception:
            src = None
        tracker = src.decode(errors="ignore").lower() if src else None
    else:
        tracker = tracker.lower()

    # 2) Кэш по источнику (если известен)
    cache_key = None
    if tracker == "rutracker":
        cache_key = f"torrent:rutracker:{topic_id}"
    elif tracker == "piratebay":
        cache_key = f"torrent:piratebay:{'hash' if is_hex_hash(topic_id) else 'id'}:{topic_id if not is_hex_hash(topic_id) else topic_id.upper()}"

    if cache_key:
        cached = await redis.get(cache_key)
        if cached:
            return StreamingResponse(
                io.BytesIO(cached),
                media_type="application/x-bittorrent",
                headers={"Content-Disposition": f'attachment; filename="{topic_id}.torrent"'},
            )

    # 3) Скачивание
    data: Optional[bytes] = None
    if tracker == "piratebay":
        # источник определён — НИКАКОГО фолбэка на RT
        data = await try_piratebay(topic_id, strict=True)
    elif tracker == "rutracker":
        # источник определён — НИКАКОГО фолбэка на PB
        data = await try_rutracker(topic_id, strict=True)
    else:
        # источник не известен: сначала PB, потом RT
        data = await try_piratebay(topic_id, strict=False)
        if data is None:
            data = await try_rutracker(topic_id, strict=False)

    if not data:
        raise HTTPException(status_code=404, detail="Torrent not found")

    # 4) Пишем кэш по реальному источнику
    try:
        src = await redis.get(f"torrent:source:{topic_id}")
        src_decoded = src.decode(errors="ignore").lower() if src else None
    except Exception:
        src_decoded = None

    if src_decoded == "rutracker":
        cache_key = f"torrent:rutracker:{topic_id}"
    elif src_decoded == "piratebay":
        cache_key = f"torrent:piratebay:{'hash' if is_hex_hash(topic_id) else 'id'}:{topic_id if not is_hex_hash(topic_id) else topic_id.upper()}"
    else:
        # по умолчанию считаем PB
        cache_key = f"torrent:piratebay:{'hash' if is_hex_hash(topic_id) else 'id'}:{topic_id if not is_hex_hash(topic_id) else topic_id.upper()}"

    await redis.set(cache_key, data, ex=300)

    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/x-bittorrent",
        headers={"Content-Disposition": f'attachment; filename="{topic_id}.torrent"'},
    )
