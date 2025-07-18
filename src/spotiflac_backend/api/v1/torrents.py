# src/spotiflac_backend/api/v1/torrents.py
from typing import List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from spotiflac_backend.services.rutracker import RutrackerService

router = APIRouter(prefix="/torrents")


class TorrentInfoResponse(BaseModel):
    title: str
    url: str
    size: str
    seeders: int
    leechers: int


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
