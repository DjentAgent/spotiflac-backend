# src/spotiflac_backend/api/v1/torrents.py

import io
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
import aioredis
from pydantic import BaseModel

from spotiflac_backend.core.config import settings
from spotiflac_backend.services.rutracker import RutrackerService

router = APIRouter(prefix="")

class TorrentInfoResponse(BaseModel):
    title: str
    url: str
    size: str
    seeders: int
    leechers: int

@router.get("/search", response_model=List[TorrentInfoResponse])
async def search_torrents(
    q: str = Query(..., title="Search query", description="Album or track name"),
    lossless: Optional[bool] = Query(
        None,
        title="Filter by format",
        description="Если true — только lossless, если false — только lossy (mp3 и т.п.), если не указан — всё"
    )
):
    svc = RutrackerService()
    try:
        return await svc.search(q, lossless)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        await svc.close()

# Redis для скачивания
redis: aioredis.Redis = aioredis.from_url(
    settings.redis_url, encoding=None, decode_responses=False
)

@router.get("/download/{topic_id}", response_class=StreamingResponse)
async def download_torrent(topic_id: int):
    cache_key = f"torrent:{topic_id}"
    data = await redis.get(cache_key)
    if data is None:
        svc = RutrackerService()
        try:
            data = await svc.download(topic_id)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Download failed: {e}")
        finally:
            await svc.close()
        await redis.set(cache_key, data, ex=300)

    headers = {"Content-Disposition": f'attachment; filename="{topic_id}.torrent"'}
    return StreamingResponse(io.BytesIO(data),
                             media_type="application/x-bittorrent",
                             headers=headers)