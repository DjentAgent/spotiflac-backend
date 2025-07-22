import io
from typing import List, Optional

import aioredis
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from spotiflac_backend.core.config import settings
from spotiflac_backend.services.rutracker import RutrackerService, CaptchaRequired

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
            # капча не нужна
            return {"session_id": "", "captcha_image": ""}
        # капча нужна — выброса исключения не произойдёт, метод бросает CaptchaRequired
        # но на всякий случай:
        raise HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": sid, "captcha_image": img_url}
        )
    except CaptchaRequired as c:
        return HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": c.session_id, "captcha_image": c.img_url}
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
    svc = RutrackerService()
    try:
        return await svc.search(q, only_lossless=lossless, track=track)
    except CaptchaRequired as c:
        raise HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": c.session_id, "captcha_image": c.img_url}
        )
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        await svc.close()

@router.get(
    "/download/{topic_id}",
    response_class=StreamingResponse,
    responses={CAPTCHA_REQUIRED: {"description": "Captcha required"}},
)
async def download_torrent(request: Request, topic_id: int):
    cache_key = f"torrent:{topic_id}"
    data = await redis.get(cache_key)
    if data is None:
        svc = RutrackerService()
        try:
            data = await svc.download(topic_id)
        except CaptchaRequired as c:
            raise HTTPException(
                status_code=CAPTCHA_REQUIRED,
                detail={"session_id": c.session_id, "captcha_image": c.img_url}
            )
        except RuntimeError as e:
            raise HTTPException(status_code=502, detail=str(e))
        finally:
            await svc.close()
        await redis.set(cache_key, data, ex=300)

    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/x-bittorrent",
        headers={"Content-Disposition": f'attachment; filename="{topic_id}.torrent"'},
    )