import asyncio
import base64
import binascii
import json
import logging
import re
from typing import List, Optional
from urllib.parse import urlparse, parse_qs

import aiohttp
import aioredis
from aiohttp.client_exceptions import ContentTypeError
from fastapi import HTTPException
from rapidfuzz import fuzz

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

# libtorrent (опционально для DHT-фолбэка)
try:
    import libtorrent as lt  # type: ignore
except Exception:
    lt = None  # фолбэк отключён, если не установлен

log = logging.getLogger(__name__)

_LOSSLESS_RE = re.compile(r"\b(flac|wavpack|wv|ape|alac|aiff|pcm|dts|mlp|tta|mqa|lossless)\b", re.IGNORECASE)
_LOSSY_RE    = re.compile(r"\b(mp3|aac|ogg|opus|lossy)\b", re.IGNORECASE)


def _human_size(nbytes: int) -> str:
    suffixes = ["B", "KB", "MB", "GB", "TB"]
    v = float(nbytes)
    for suf in suffixes:
        if v < 1024.0:
            return f"{v:.2f} {suf}"
        v /= 1024.0
    return f"{v:.2f} PB"


def _hex_upper_from_btih(btih: str) -> Optional[str]:
    v = (btih or "").strip()
    if v.lower().startswith("urn:btih:"):
        v = v.split(":", 2)[-1]
    if v.lower().startswith("urn:btmh:"):
        # BitTorrent v2 (sha256) – кеши .torrent обычно не отдают такие файлы
        return None

    if len(v) == 40 and all(c in "0123456789abcdefABCDEF" for c in v):
        return v.upper()

    if len(v) == 32:
        try:
            raw = base64.b32decode(v.upper())
            return binascii.hexlify(raw).decode("ascii").upper()
        except Exception:
            return None

    return None


class PirateBayService:
    def __init__(
        self,
        api_base_url: Optional[str] = None,
        redis_url:    Optional[str] = None,
        max_filelist_concurrency: int = 10,
        dht_timeout_sec: int = 60,  # таймаут DHT-фолбэка
    ) -> None:
        base = api_base_url or getattr(settings, "piratebay_api_base", "https://apibay.org")
        self.api_base = base.rstrip("/")

        self.http = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20),
            headers={
                "Accept": "application/json, text/html;q=0.9,*/*;q=0.8",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            },
        )

        r_url = redis_url or getattr(settings, "redis_url", "redis://localhost:6379/0")
        self.redis = aioredis.from_url(r_url, encoding="utf-8", decode_responses=True)

        self.search_ttl   = int(getattr(settings, "piratebay_search_ttl",   5 * 60))
        self.filelist_ttl = int(getattr(settings, "piratebay_filelist_ttl", 24 * 3600))
        self.torrent_ttl  = int(getattr(settings, "piratebay_torrent_ttl",  7 * 24 * 3600))
        self.id2hash_ttl  = int(getattr(settings, "piratebay_id2hash_ttl",  24 * 3600))

        self._fl_sem = asyncio.Semaphore(max_filelist_concurrency)
        self._mirrors = [
            "https://itorrents.org/torrent/{HEX}.torrent",
            "https://btcache.me/torrent/{HEX}.torrent",
            "https://torrage.info/torrent/{HEX}.torrent",
        ]
        self._dht_timeout_sec = int(dht_timeout_sec)

    # ----------------- cache helpers (torrent blob) -----------------
    async def _cache_get_blob(self, key: str) -> Optional[bytes]:
        s = await self.redis.get(key)
        if not s:
            return None
        try:
            return base64.b64decode(s)
        except Exception:
            log.exception("Failed to b64-decode blob cache for %s", key)
            return None

    async def _cache_set_blob(self, key: str, data: bytes, ex: int) -> None:
        b64 = base64.b64encode(data).decode("ascii")
        await self.redis.set(key, b64, ex=ex)

    # ------------------------------ search ------------------------------
    async def search(
        self,
        query: str,
        only_lossless: Optional[bool] = None,
        track: Optional[str] = None,
    ) -> List[TorrentInfo]:
        cache_key = f"pb:search:{query}:{only_lossless}:{track}"
        if cached := await self.redis.get(cache_key):
            try:
                data = json.loads(cached)
                return [TorrentInfo(**d) for d in data]
            except Exception:
                log.exception("Failed to load search cache %s", cache_key)

        url = f"{self.api_base}/q.php"
        params = {"q": query, "cat": 100}  # Audio
        try:
            async with self.http.get(url, params=params) as resp:
                resp.raise_for_status()
                items = await resp.json()
        except Exception as e:
            log.exception("Search API error: %s", e)
            raise HTTPException(status_code=502, detail="PirateBay API error")

        filtered = []
        for it in items:
            name = it.get("name", "") or ""
            is_l = bool(_LOSSLESS_RE.search(name))
            is_y = bool(_LOSSY_RE.search(name))
            if only_lossless is True and (not is_l or is_y):
                continue
            if only_lossless is False and is_l:
                continue
            filtered.append(it)

        if track:
            track_low = track.lower()

            async def check(it: dict) -> Optional[dict]:
                async with self._fl_sem:
                    fl = await self._get_filelist(str(it["id"]))
                for fname in fl:
                    f_low = fname.lower()
                    if track_low in f_low or fuzz.partial_ratio(track_low, f_low) >= 80:
                        return it
                return None

            results = await asyncio.gather(*(check(it) for it in filtered))
            filtered = [it for it in results if it]

        out: List[dict] = []
        infos: List[TorrentInfo] = []
        for it in filtered:
            size_bytes = int(it.get("size", 0) or 0)
            page_url = f"https://thepiratebay.org/?t={it['id']}"  # критично для единого роутера
            info = TorrentInfo(
                title    = it.get("name", "") or "",
                url      = page_url,
                size     = _human_size(size_bytes),
                seeders  = int(it.get("seeders", 0) or 0),
                leechers = int(it.get("leechers", 0) or 0),
            )
            infos.append(info)
            out.append(info.__dict__)

        await self.redis.set(cache_key, json.dumps(out), ex=self.search_ttl)
        return infos

    # ------------------------------ filelist ------------------------------
    async def _get_filelist(self, torrent_id: str) -> List[str]:
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
                try:
                    files = await resp.json(content_type=None)
                except ContentTypeError:
                    text = await resp.text()
                    files = json.loads(text)
        except (ContentTypeError, json.JSONDecodeError) as e:
            log.exception("Filelist parse error: %s", e)
            raise HTTPException(status_code=502, detail="PirateBay filelist parse error")
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

    # ------------------------------ info_hash resolve ------------------------------
    async def _resolve_info_hash_from_id(self, torrent_id: str) -> Optional[str]:
        cache_key = f"pb:id2hash:{torrent_id}"
        if cached := await self.redis.get(cache_key):
            ih = (cached or "").strip()
            if ih:
                return ih

        # 1) apibay t.php
        try:
            async with self.http.get(f"{self.api_base}/t.php", params={"id": torrent_id}) as resp:
                resp.raise_for_status()
                meta = await resp.json()
                ih = meta.get("info_hash") or meta.get("infohash") or ""
                ih_norm = _hex_upper_from_btih(ih)
                if ih_norm:
                    await self.redis.set(cache_key, ih_norm, ex=self.id2hash_ttl)
                    return ih_norm
        except Exception as e:
            log.warning("t.php error for id=%s: %s", torrent_id, e)

        # 2) fallback с HTML-страниц
        for url in (
            f"https://thepiratebay.org/?t={torrent_id}",
            f"https://thepiratebay.org/description.php?id={torrent_id}",
        ):
            ih_fb = await self._btih_from_page(url)
            if ih_fb:
                await self.redis.set(cache_key, ih_fb, ex=self.id2hash_ttl)
                return ih_fb

        return None

    async def _btih_from_page(self, page_url: str) -> Optional[str]:
        try:
            async with self.http.get(page_url) as resp:
                if resp.status != 200:
                    return None
                html = await resp.text()
        except Exception as e:
            log.warning("TPB page fetch failed for %s: %s", page_url, e)
            return None

        m = re.search(
            r'href=["\']magnet:\?[^"\']*xt=urn:btih:([0-9A-Za-z]{32,40})',
            html, re.IGNORECASE
        )
        if not m:
            return None
        return _hex_upper_from_btih(m.group(1))

    def _extract_btih_from_url(self, url: str) -> Optional[str]:
        parsed = urlparse(url)
        if url.startswith("magnet:"):
            qs = parse_qs(parsed.query)
            for xt in qs.get("xt", []):
                if xt.lower().startswith("urn:btih:"):
                    ih = _hex_upper_from_btih(xt)
                    if ih:
                        return ih
                if xt.lower().startswith("urn:btmh:"):
                    return None
            return None
        return None

    # ------------------------------ DHT fallback ------------------------------
    def _dht_fetch_torrent_blocking(self, info_hash_hex: str, timeout_sec: int) -> Optional[bytes]:
        """Блокирующий DHT-фетч через libtorrent: получаем метадату и собираем .torrent."""
        if lt is None:
            return None

        ses = lt.session()
        try:
            # включаем DHT
            ses.listen_on(6881, 6891)
            try:
                ses.add_dht_router("router.bittorrent.com", 6881)
                ses.add_dht_router("router.utorrent.com", 6881)
                ses.add_dht_router("dht.transmissionbt.com", 6881)
                ses.add_dht_router("dht.aelitis.com", 6881)
            except Exception:
                pass
            ses.start_dht()

            # добавляем magnet по хешу
            uri = f"magnet:?xt=urn:btih:{info_hash_hex}"
            params = lt.parse_magnet_uri(uri)
            params.save_path = "."  # не скачиваем, только метадата
            h = ses.add_torrent(params)

            import time
            deadline = time.time() + max(10, timeout_sec)
            while time.time() < deadline and not h.has_metadata():
                time.sleep(0.3)

            if not h.has_metadata():
                return None

            ti = h.get_torrent_info()
            # собираем .torrent
            ct = lt.create_torrent(ti)
            # можно добавить парочку публичных трекеров (необязательно)
            for tr in [
                "udp://tracker.opentrackr.org:1337/announce",
                "udp://open.stealth.si:80/announce",
            ]:
                try:
                    ct.add_tracker(tr)
                except Exception:
                    pass

            torrent_dict = ct.generate()
            data = lt.bencode(torrent_dict)
            return bytes(data)
        finally:
            try:
                ses.pause()
                ses.stop_dht()
            except Exception:
                pass

    async def _dht_fetch_torrent(self, info_hash_hex: str, timeout_sec: int) -> Optional[bytes]:
        return await asyncio.to_thread(self._dht_fetch_torrent_blocking, info_hash_hex, timeout_sec)

    # ------------------------------ download ------------------------------
    async def download(self, info: TorrentInfo) -> bytes:
        # 1) magnet → btih
        info_hash_hex = self._extract_btih_from_url(info.url)

        # 2) ?t=/id → btih
        if not info_hash_hex:
            parsed = urlparse(info.url)
            qs = parse_qs(parsed.query)
            tid = None
            for key in ("t", "id"):
                if key in qs and qs[key]:
                    tid = qs[key][0]
                    break
            if tid:
                info_hash_hex = await self._resolve_info_hash_from_id(tid)

        if not info_hash_hex:
            raise HTTPException(
                status_code=422,
                detail="Cannot resolve BTIH (v1). Torrent may be BitTorrent v2 (btmh) or missing."
            )

        cache_key = f"pb:blob:{info_hash_hex}"
        if cached := await self._cache_get_blob(cache_key):
            return cached

        # 3) пробуем кеш-зеркала
        last_exc: Optional[Exception] = None
        for tmpl in self._mirrors:
            url = tmpl.replace("{HEX}", info_hash_hex)
            try:
                async with self.http.get(url) as r:
                    data = await r.read()
                    if r.status == 200 and (
                        r.headers.get("Content-Type", "").startswith("application/x-bittorrent")
                        or data.startswith(b"d")
                    ):
                        await self._cache_set_blob(cache_key, data, ex=self.torrent_ttl)
                        return data
            except Exception as e:
                last_exc = e
                continue

        # 4) фолбэк через DHT (если доступен libtorrent)
        dht_data = await self._dht_fetch_torrent(info_hash_hex, self._dht_timeout_sec)
        if dht_data:
            await self._cache_set_blob(cache_key, dht_data, ex=self.torrent_ttl)
            return dht_data

        detail = "Torrent not found in public caches"
        if last_exc is not None:
            detail += f" (last error: {last_exc})"
        if lt is None:
            detail += "; DHT fallback unavailable (libtorrent not installed)"
        raise HTTPException(status_code=404, detail=detail)

    async def download_by_id(self, torrent_id: str) -> bytes:
        ih = await self._resolve_info_hash_from_id(torrent_id)
        if not ih:
            raise HTTPException(status_code=404, detail="Metadata not found or unsupported (btmh/v2)")
        ti = TorrentInfo(title="", url=f"https://thepiratebay.org/?t={torrent_id}",
                         size="0 B", seeders=0, leechers=0)
        return await self.download(ti)

    async def download_by_hash(self, info_hash: str) -> bytes:
        ih = _hex_upper_from_btih(info_hash)
        if not ih:
            raise HTTPException(status_code=422, detail="Invalid BTIH (expect 40-char hex or 32-char base32)")
        ti = TorrentInfo(title="", url=f"magnet:?xt=urn:btih:{ih}",
                         size="0 B", seeders=0, leechers=0)
        return await self.download(ti)

    async def close(self) -> None:
        await self.http.close()
        await self.redis.close()

    def __del__(self):
        if getattr(self, "http", None) and not self.http.closed:
            loop = asyncio.get_event_loop()
            loop.create_task(self.http.close())