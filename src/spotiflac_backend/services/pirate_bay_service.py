import asyncio
import json
import logging
import re
from typing import List, Optional
from urllib.parse import urlparse, parse_qs

import aiohttp
import aioredis
import requests
from fastapi import HTTPException
from rapidfuzz import fuzz

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

log = logging.getLogger(__name__)

# Регулярные выражения для lossless/lossy
_LOSSLESS_RE = re.compile(
    r"\b(flac|wavpack|wv|ape|alac|aiff|pcm|dts|mlp|tta|mqa|lossless)\b",
    re.IGNORECASE,
)
_LOSSY_RE = re.compile(
    r"\b(mp3|aac|ogg|opus|lossy)\b",
    re.IGNORECASE,
)


def _human_size(nbytes: int) -> str:
    suffixes = ["B", "KB", "MB", "GB", "TB"]
    v = float(nbytes)
    for suf in suffixes:
        if v < 1024.0:
            return f"{v:.2f} {suf}"
        v /= 1024.0
    return f"{v:.2f} PB"


class PirateBayService:
    def __init__(
            self,
            api_base_url: Optional[str] = None,
            redis_url: Optional[str] = None,
            max_filelist_concurrency: int = 10,
    ) -> None:
        # Подхватываем из settings, если не передано явно
        base = api_base_url or getattr(settings, "piratebay_api_base", "https://apibay.org")
        self.api_base = base.rstrip("/")

        # Асинхронный HTTP-клиент
        self.http = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20),
            headers={"Accept": "application/json"},
        )

        # Асинхронный Redis
        r_url = redis_url or getattr(settings, "redis_url", "redis://localhost:6379/0")
        self.redis = aioredis.from_url(r_url, encoding="utf-8", decode_responses=True)

        # TTL кешей
        self.search_ttl = int(getattr(settings, "piratebay_search_ttl", 5 * 60))
        self.filelist_ttl = int(getattr(settings, "piratebay_filelist_ttl", 24 * 3600))
        self.torrent_ttl = int(getattr(settings, "piratebay_torrent_ttl", 7 * 24 * 3600))

        # Семафор для ограничения одновременных filelist-запросов
        self._fl_sem = asyncio.Semaphore(max_filelist_concurrency)

    async def search(
            self,
            query: str,
            only_lossless: Optional[bool] = None,
            track: Optional[str] = None,
    ) -> List[TorrentInfo]:
        """Асинхронный поиск с опциональным фильтром lossless и по названию трека."""
        cache_key = f"pb:search:{query}:{only_lossless}:{track}"
        if cached := await self.redis.get(cache_key):
            try:
                data = json.loads(cached)
                return [TorrentInfo(**d) for d in data]
            except Exception:
                log.exception("Failed to load search cache %s", cache_key)

        # 1) запрос q.php
        url = f"{self.api_base}/q.php"
        params = {"q": query, "cat": 0}
        try:
            async with self.http.get(url, params=params) as resp:
                resp.raise_for_status()
                items = await resp.json()
        except Exception as e:
            log.exception("Search API error: %s", e)
            raise HTTPException(status_code=502, detail="PirateBay API error")

        # 2) фильтр lossless/lossy
        filtered = []
        for it in items:
            name = it.get("name", "")
            is_l = bool(_LOSSLESS_RE.search(name))
            is_y = bool(_LOSSY_RE.search(name))
            if only_lossless is True and (not is_l or is_y):
                continue
            if only_lossless is False and is_l:
                continue
            filtered.append(it)

        # 3) фильтр по треку (если нужно) — параллельно filelist
        if track:
            track_low = track.lower()

            async def check(it: dict) -> Optional[dict]:
                async with self._fl_sem:
                    fl = await self._get_filelist(it["id"])
                for fname in fl:
                    f_low = fname.lower()
                    if track_low in f_low or fuzz.partial_ratio(track_low, f_low) >= 80:
                        return it
                return None

            coros = [check(it) for it in filtered]
            results = await asyncio.gather(*coros)
            filtered = [it for it in results if it]

        # 4) строим TorrentInfo и кешируем
        out: List[dict] = []
        infos: List[TorrentInfo] = []
        for it in filtered:
            size_bytes = int(it.get("size", 0))
            info = TorrentInfo(
                title=it.get("name", ""),
                url=f"https://thepiratebay.org/?t={it['id']}",
                size=_human_size(size_bytes),
                seeders=int(it.get("seeders", 0)),
                leechers=int(it.get("leechers", 0)),
            )
            infos.append(info)
            out.append(info.__dict__)  # <-- заменили .dict() на __dict__()

        await self.redis.set(cache_key, json.dumps(out), ex=self.search_ttl)
        return infos

    async def _get_filelist(self, torrent_id: str) -> List[str]:
        """Асинхронно получает и кеширует список файлов через f.php."""
        cache_key = f"pb:fl:{torrent_id}"
        if cached := await self.redis.get(cache_key):
            try:
                return json.loads(cached)
            except Exception:
                log.exception("Failed to load filelist cache %s", cache_key)

        url = f"{self.api_base}/f.php"
        try:
            async with self.http.get(url, params={"id": torrent_id}) as resp:
                resp.raise_for_status()
                files = await resp.json()
        except Exception as e:
            log.exception("Filelist API error: %s", e)
            raise HTTPException(status_code=502, detail="PirateBay filelist error")

        names: List[str] = []
        for f in files:
            nl = f.get("name")
            if isinstance(nl, list) and nl:
                names.append(nl[0])

        await self.redis.set(cache_key, json.dumps(names), ex=self.filelist_ttl)
        return names

    async def download(self, info: TorrentInfo) -> bytes:
        # 1) extract the info-hash
        parsed = urlparse(info.url)
        qs = parse_qs(parsed.query)
        info_hash = None

        # if you stuffed ?t=<id> into the URL, first fetch metadata to get the hash
        if "t" in qs and not info.url.startswith("magnet:"):
            tid = qs["t"][0]
            async with self.http.get(f"{self.api_base}/t.php", params={"id": tid}) as meta_resp:
                meta_resp.raise_for_status()
                meta = await meta_resp.json()
            info_hash = meta.get("info_hash")
        else:
            # maybe it's already a magnet? look for xt=urn:btih:<hash>
            for xt in qs.get("xt", []):
                if xt.startswith("urn:btih:"):
                    info_hash = xt.split(":", 2)[-1].upper()
                    break

        if not info_hash:
            raise HTTPException(status_code=400, detail="No info hash found")

        cache_key = f"pb:blob:{info_hash}"
        # 2) try cache
        if cached := await self.redis.get(cache_key):
            return cached if isinstance(cached, (bytes, bytearray)) else cached.encode()

        # 3) try mirrors via aiohttp
        mirrors = [
            f"https://itorrents.org/torrent/{info_hash}.torrent",
            f"https://torrage.info/torrent/{info_hash}.torrent",
            f"https://btcache.me/torrent/{info_hash}",
        ]
        last_exc = None
        for url in mirrors:
            try:
                async with self.http.get(url) as torrent_resp:
                    if torrent_resp.status == 200 and (
                            "application/x-bittorrent" in torrent_resp.headers.get("Content-Type", "")
                            or (await torrent_resp.content.read(12)).startswith(b"d8:announce")
                    ):
                        # rewind the stream if you peeked
                        data = await torrent_resp.read()
                        await self.redis.set(cache_key, data, ex=self.torrent_ttl)
                        return data
            except Exception as e:
                last_exc = e
                continue

        detail = "Torrent not found" if last_exc is None else f"Error fetching torrent: {last_exc}"
        raise HTTPException(status_code=404, detail=detail)

    async def download_by_id(self, torrent_id: str) -> bytes:
        """Асинхронная загрузка по id через t.php и download."""
        async with self.http.get(f"{self.api_base}/t.php", params={"id": torrent_id}) as resp:
            resp.raise_for_status()
            meta = await resp.json()
        info_hash = meta.get("info_hash")
        if not info_hash:
            raise HTTPException(status_code=404, detail="Metadata not found")
        ti = TorrentInfo(
            title="",
            url=f"magnet:?xt=urn:btih:{info_hash}",
            size="0 B",
            seeders=0,
            leechers=0,
        )
        return await self.download(ti)

    async def download_by_hash(self, info_hash: str) -> bytes:
        """Асинхронная загрузка по хэшу."""
        ti = TorrentInfo(
            title="",
            url=f"magnet:?xt=urn:btih:{info_hash}",
            size="0 B",
            seeders=0,
            leechers=0,
        )
        return await self.download(ti)

    async def close(self) -> None:
        """Закрыть HTTP-сессию и Redis-подключение."""
        await self.http.close()
        await self.redis.close()

    def __del__(self):
        # Если закрытие не было выполнено явно
        if not self.http.closed:
            loop = asyncio.get_event_loop()
            loop.create_task(self.http.close())
        # aioredis Connector закрывается вместе с сессией
