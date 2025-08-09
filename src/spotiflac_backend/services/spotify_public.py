# spotiflac_backend/services/spotify_public.py
import asyncio
import base64
import os
import time
from typing import Any, Dict, List, Optional

import aiohttp

# стараемся брать из settings, но работаем и без него
try:
    from spotiflac_backend.core.config import settings  # type: ignore
except Exception:
    settings = None  # fallback ниже

SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE = "https://api.spotify.com/v1"


def _cfg(name: str, default=None):
    if settings and hasattr(settings, name):
        return getattr(settings, name)
    # env fallback (вдруг settings не расширяли)
    env_name = name.upper()
    return os.getenv(env_name, default)


class SpotifyPublicService:
    """
    Лёгкий клиент Spotify Web API по app-токену (Client Credentials).
    Без логина пользователя.
    """

    def __init__(self) -> None:
        timeout = float(_cfg("spotify_timeout", 8.0))
        self.market: Optional[str] = _cfg("spotify_market", "US")
        self.proxy: Optional[str] = _cfg("spotify_http_proxy", None)
        self.cache_ttl: int = int(_cfg("spotify_cache_ttl", 120))

        self._client_id: Optional[str] = _cfg("spotify_client_id", None)
        self._client_secret: Optional[str] = _cfg("spotify_client_secret", None)

        self._sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout))
        self._token: Optional[str] = None
        self._exp: float = 0.0

        # очень простой in-memory кэш для /search и /tracks
        self._cache: Dict[str, tuple[float, Any]] = {}
        self._lock = asyncio.Lock()

    async def close(self):
        await self._sess.close()

    async def _ensure_token(self) -> str:
        async with self._lock:
            if self._token and time.time() < self._exp - 30:
                return self._token

            if not self._client_id or not self._client_secret:
                raise RuntimeError("Spotify client credentials are not configured")

            basic = base64.b64encode(
                f"{self._client_id}:{self._client_secret}".encode()
            ).decode()

            async with self._sess.post(
                SPOTIFY_TOKEN_URL,
                headers={"Authorization": f"Basic {basic}",
                         "Content-Type": "application/x-www-form-urlencoded"},
                data={"grant_type": "client_credentials"},
                proxy=self.proxy,
            ) as resp:
                data = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"Spotify token error {resp.status}: {data}")
                self._token = data["access_token"]
                self._exp = time.time() + int(data.get("expires_in", 3600))
                return self._token

    async def _request(self, path: str, params: Dict[str, Any]) -> Any:
        # 1 попытка с текущим токеном, при 401 — обновить и повторить
        token = await self._ensure_token()
        url = f"{SPOTIFY_API_BASE}{path}"
        headers = {"Authorization": f"Bearer {token}"}

        for attempt in (1, 2):
            async with self._sess.get(
                url, headers=headers, params=params, proxy=self.proxy
            ) as r:
                if r.status == 401 and attempt == 1:
                    # токен протух — обновим
                    await self._ensure_token()
                    headers["Authorization"] = f"Bearer {self._token}"
                    continue
                if r.status == 429:
                    retry = int(r.headers.get("Retry-After", "1"))
                    await asyncio.sleep(retry)
                    continue
                data = await r.json()
                if r.status >= 400:
                    raise RuntimeError(f"Spotify API {r.status}: {data}")
                return data
        raise RuntimeError("Spotify API request failed after retries")

    def _cache_get(self, key: str):
        item = self._cache.get(key)
        if not item:
            return None
        ts, val = item
        if time.time() - ts > self.cache_ttl:
            self._cache.pop(key, None)
            return None
        return val

    def _cache_put(self, key: str, val: Any):
        self._cache[key] = (time.time(), val)

    # ---------- mapping ----------

    @staticmethod
    def _map_track_item(item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": item.get("id"),
            "title": item.get("name"),
            "artists": [{"name": a.get("name")} for a in item.get("artists", [])],
            "album": {
                "images": [{"url": im.get("url")} for im in item.get("album", {}).get("images", [])]
            },
        }

    @staticmethod
    def _map_track_detail(item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": item.get("id"),
            "title": item.get("name"),
            "artists": [{"name": a.get("name")} for a in item.get("artists", [])],
            "album": {
                "name": item.get("album", {}).get("name"),
                "images": [{"url": im.get("url")} for im in item.get("album", {}).get("images", [])],
            },
            "duration_ms": item.get("duration_ms"),
            "popularity": item.get("popularity"),
            "preview_url": item.get("preview_url"),
        }

    # ---------- public methods ----------

    async def search_tracks(self, q: str, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        key = f"search:{q}:{limit}:{offset}:{self.market}"
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        params = {
            "q": q,
            "type": "track",
            "limit": max(1, min(limit, 50)),
            "offset": max(0, offset),
        }
        if self.market:
            params["market"] = self.market

        data = await self._request("/search", params)
        tracks = data.get("tracks", {})
        mapped = {
            "tracksPage": {
                "href": tracks.get("href"),
                "items": [self._map_track_item(it) for it in tracks.get("items", [])],
                "limit": tracks.get("limit", 0),
                "next": tracks.get("next"),
                "offset": tracks.get("offset", 0),
                "total": tracks.get("total", 0),
            }
        }
        self._cache_put(key, mapped)
        return mapped

    async def get_track_detail(self, track_id: str) -> Dict[str, Any]:
        key = f"track:{track_id}"
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        params = {}
        if self.market:
            params["market"] = self.market
        data = await self._request(f"/tracks/{track_id}", params)
        mapped = self._map_track_detail(data)
        self._cache_put(key, mapped)
        return mapped