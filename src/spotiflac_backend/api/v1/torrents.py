import json
import io
import os
from typing import Any, Dict, List, Optional

import aioredis
from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from spotiflac_backend.core.config import settings
from spotiflac_backend.services.rutracker import RutrackerService

router = APIRouter(prefix="")

CAPTCHA_REQUIRED = 428

# Pydantic‑модели
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

# Настраиваем aioredis
redis: aioredis.Redis = aioredis.from_url(
    settings.redis_url, encoding="utf-8", decode_responses=False
)

@router.post("/login/initiate", response_model=CaptchaInitResponse, responses={CAPTCHA_REQUIRED: {"model": CaptchaInitResponse}})
async def login_initiate():
    """
    Инициализирует вход: если нужна капча, возвращает session_id и путь до картинки.
    """
    svc = RutrackerService()
    try:
        try:
            session_id, img_path = svc.initiate_login()
            # Если капчи нет, сразу возвращаем пустой ответ (не должно случаться в текущем flow)
            return {"session_id": session_id, "captcha_image": img_path}
        except RuntimeError as e:
            msg = str(e)
            if msg.startswith("CAPTCHA_REQUIRED:"):
                session_id = msg.split(":", 1)[1]
                # возвращаем 428 с телом, чтобы клиент поймал
                raise HTTPException(
                    status_code=CAPTCHA_REQUIRED,
                    detail={"session_id": session_id, "captcha_image": os.path.join(settings.captcha_base_url, f"captcha-{session_id}.png")}
                )
            # иначе пробрасываем
            raise
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
    q: str = Query(..., title="Search query"),
    lossless: Optional[bool] = Query(None, title="Only lossless"),
    track: Optional[str] = Query(None, title="Track name"),
):
    svc = RutrackerService()
    try:
        return await svc.search(q, only_lossless=lossless, track=track)
    except RuntimeError as e:
        msg = str(e)
        if msg.startswith("CAPTCHA_REQUIRED:"):
            session_id = msg.split(":", 1)[1]
            raise HTTPException(status_code=CAPTCHA_REQUIRED, detail={"session_id": session_id})
        raise HTTPException(status_code=502, detail=msg)
    finally:
        await svc.close()

@router.get(
    "/download/{topic_id}",
    response_class=StreamingResponse,
    responses={CAPTCHA_REQUIRED: {"description": "Captcha required"}},
)
async def download_torrent(topic_id: int):
    cache_key = f"torrent:{topic_id}"
    data = await redis.get(cache_key)
    if data is None:
        svc = RutrackerService()
        try:
            data = await svc.download(topic_id)
        except RuntimeError as e:
            msg = str(e)
            if msg.startswith("CAPTCHA_REQUIRED:"):
                session_id = msg.split(":", 1)[1]
                raise HTTPException(status_code=CAPTCHA_REQUIRED, detail={"session_id": session_id})
            raise HTTPException(status_code=502, detail=f"Download failed: {msg}")
        finally:
            await svc.close()
        await redis.set(cache_key, data, ex=300)

    headers = {"Content-Disposition": f'attachment; filename="{topic_id}.torrent"'}
    return StreamingResponse(io.BytesIO(data), media_type="application/x-bittorrent", headers=headers)