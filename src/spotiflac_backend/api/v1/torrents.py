"""
FastAPI router providing endpoints for authentication, searching and
downloading torrents from RuTracker and The Pirate Bay.  This module
extends the existing RuTracker endpoints by adding search and download
support for Pirate Bay while preserving backwards compatibility.

The /search endpoint queries RuTracker, /search/piratebay queries
Pirate Bay, and /download/{topic_id} serves `.torrent` files from both
trackers depending on the ``tracker`` query parameter or the format of
``topic_id``.
"""

import io
import re
import urllib.parse
from typing import List, Optional

import aioredis
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from spotiflac_backend.core.config import settings
from spotiflac_backend.services.rutracker import RutrackerService, CaptchaRequired
from spotiflac_backend.services.pirate_bay_service import PirateBayService

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


# Initialise asynchronous Redis client.  decode_responses=False ensures we
# receive bytes for torrent blobs; keys remain strings.
redis: aioredis.Redis = aioredis.from_url(
    settings.redis_url, encoding="utf-8", decode_responses=False
)


@router.post(
    "/login/initiate",
    response_model=CaptchaInitResponse,
    responses={CAPTCHA_REQUIRED: {"model": CaptchaInitResponse}},
)
async def login_initiate(request: Request):
    """Begin RuTracker login and return CAPTCHA info if required."""
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
            detail={"session_id": sid, "captcha_image": img_url},
        )
    except CaptchaRequired as c:
        return HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": c.session_id, "captcha_image": c.img_url},
        )
    finally:
        await svc.close()


@router.post("/login/complete")
async def login_complete(body: CaptchaCompleteRequest):
    """Submit CAPTCHA solution and complete RuTracker login."""
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
    """Search RuTracker for torrents matching the query."""
    svc = RutrackerService()
    try:
        # Perform the search
        results = await svc.search(q, only_lossless=lossless, track=track)
        # For each result, extract the topic ID from the ``url`` field and store
        # the tracker source in Redis.  This allows the download endpoint to
        # infer the correct tracker when ``tracker`` parameter is omitted.
        try:
            # Use a moderate TTL for the source mapping; 24 hours by default
            source_ttl = 24 * 3600
            for res in results:
                if not res.url:
                    continue
                parsed = urllib.parse.urlparse(res.url)
                qs = urllib.parse.parse_qs(parsed.query)
                tid_list = qs.get("t")
                if tid_list:
                    tid = tid_list[0]
                    # Store the source as bytes so that decode_responses=False is satisfied
                    await redis.setex(f"torrent:source:{tid}", source_ttl, b"rutracker")
        except Exception:
            # Ignore mapping errors
            pass
        return results
    except CaptchaRequired as c:
        raise HTTPException(
            status_code=CAPTCHA_REQUIRED,
            detail={"session_id": c.session_id, "captcha_image": c.img_url},
        )
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))
    finally:
        await svc.close()


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
    """Search The Pirate Bay for torrents matching the query.

    This endpoint does not require a CAPTCHA and will forward API errors
    as 502 responses.
    """
    svc = PirateBayService()
    try:
        results = await svc.search(q, only_lossless=lossless, track=track)
        # Map Pirate Bay IDs to their source in Redis for automatic tracker detection
        try:
            source_ttl = 24 * 3600
            for res in results:
                if not res.url:
                    continue
                parsed = urllib.parse.urlparse(res.url)
                qs = urllib.parse.parse_qs(parsed.query)
                tid_list = qs.get("t")
                if tid_list:
                    tid = tid_list[0]
                    await redis.setex(f"torrent:source:{tid}", source_ttl, b"piratebay")
        except Exception:
            pass
        return results
    except HTTPException as e:
        # propagate HTTPException from the service
        raise e
    except Exception as e:
        # wrap unexpected errors
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
        description="Source tracker: 'rutracker' or 'piratebay'. If omitted, the service will attempt to infer the source automatically.",
    ),
):
    """Download a .torrent file from RuTracker or Pirate Bay.

    When ``tracker`` is set to ``rutracker``, the numeric ``topic_id`` is used
    with :class:`RutrackerService`.  When ``tracker`` is ``piratebay``, the
    ``topic_id`` may be either a numeric Pirate Bay ID or a 40‑character
    hexadecimal info hash.  If ``tracker`` is omitted, the handler will
    attempt RuTracker for purely numeric IDs and Pirate Bay otherwise.
    """
    # Determine the cache key prefix based on the tracker and ID format
    def is_hex_hash(s: str) -> bool:
        return len(s) == 40 and re.fullmatch(r"[0-9a-fA-F]{40}", s) is not None

    # infer tracker if not provided by consulting cached search source
    if tracker is None:
        inferred_tracker: str
        try:
            source = await redis.get(f"torrent:source:{topic_id}")
        except Exception:
            source = None
        if source:
            # decode_responses=False -> source is bytes
            try:
                inferred_tracker = source.decode().lower()
            except Exception:
                inferred_tracker = str(source).lower()
        else:
            # Fallback: heuristically determine tracker by the format of the ID
            if topic_id.isdigit():
                inferred_tracker = "rutracker"
            else:
                inferred_tracker = "piratebay"
    else:
        inferred_tracker = tracker.lower()

    # Build a unique cache key to avoid collisions between trackers
    if inferred_tracker == "rutracker":
        cache_key = f"torrent:rutracker:{topic_id}"
    elif inferred_tracker == "piratebay":
        if is_hex_hash(topic_id):
            cache_key = f"torrent:piratebay:hash:{topic_id.upper()}"
        else:
            cache_key = f"torrent:piratebay:id:{topic_id}"
    else:
        # Unsupported tracker value
        raise HTTPException(status_code=400, detail="Unknown tracker specified")

    # Try to get the torrent blob from cache
    data = await redis.get(cache_key)
    if data is None:
        # Attempt to download from the appropriate service
        if inferred_tracker == "rutracker":
            # RuTracker expects an integer topic_id
            if not topic_id.isdigit():
                raise HTTPException(status_code=400, detail="Invalid RuTracker topic ID")
            svc = RutrackerService()
            try:
                data = await svc.download(int(topic_id))
            except CaptchaRequired as c:
                raise HTTPException(
                    status_code=CAPTCHA_REQUIRED,
                    detail={"session_id": c.session_id, "captcha_image": c.img_url},
                )
            except RuntimeError as e:
                raise HTTPException(status_code=502, detail=str(e))
            finally:
                await svc.close()
        else:
            # Pirate Bay
            pb_svc = PirateBayService()
            try:
                if is_hex_hash(topic_id):
                    data = await pb_svc.download_by_hash(topic_id)
                else:
                    # Try by ID; this will internally get info hash via t.php
                    data = await pb_svc.download_by_id(topic_id)
            except HTTPException as e:
                # Propagate known errors (404, etc.)
                raise e
            except Exception as e:
                raise HTTPException(status_code=502, detail=str(e))
        # Store in cache if download succeeded
        if data:
            await redis.set(cache_key, data, ex=300)
        else:
            # Should not happen: if data is still None
            raise HTTPException(status_code=404, detail="Torrent not found")

    # ``data`` is bytes; wrap in StreamingResponse
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/x-bittorrent",
        headers={"Content-Disposition": f'attachment; filename="{topic_id}.torrent"'},
    )