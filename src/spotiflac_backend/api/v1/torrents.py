# src/spotiflac_backend/api/v1/torrents.py

import io
from typing import List

import aioredis
from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from spotiflac_backend.core.config import settings
from spotiflac_backend.services.rutracker import RutrackerService

router = APIRouter(prefix="")

# Pydantic‑модель для списка результатов поиска
class TorrentInfoResponse(BaseModel):
    title: str
    url: str
    size: str
    seeders: int
    leechers: int


# Роут поиска остаётся без изменений
@router.get("/search", response_model=List[TorrentInfoResponse])
async def search_torrents(
    q: str = Query(..., title="Search query", description="Album or track name")
):
    svc = RutrackerService()
    try:
        return await svc.search(q)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        await svc.close()


# Подключаем Redis (заранее в settings.redis_url сконфигурирован URL вида "redis://...")
redis: aioredis.Redis = aioredis.from_url(
    settings.redis_url, encoding=None, decode_responses=False
)


@router.get("/download/{topic_id}", response_class=StreamingResponse)
async def download_torrent(topic_id: int):
    """
    Возвращает готовый .torrent для заданного topic_id.
    Сначала пробуем из кеша, иначе грузим с rutracker и кешируем на 5 минут.
    """
    cache_key = f"torrent:{topic_id}"
    # 1) Попробуем из Redis
    data = await redis.get(cache_key)
    if data is None:
        # 2) Если в кеше нет — загрузим с rutracker
        svc = RutrackerService()
        try:
            data = await svc.download(topic_id)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Download failed: {e}")
        finally:
            await svc.close()
        # 3) Сохраним в кеш на 300 секунд
        await redis.set(cache_key, data, ex=300)

    # 4) Отдаём как StreamingResponse
    headers = {
        "Content-Disposition": f'attachment; filename="{topic_id}.torrent"'
    }
    return StreamingResponse(io.BytesIO(data),
                             media_type="application/x-bittorrent",
                             headers=headers)
