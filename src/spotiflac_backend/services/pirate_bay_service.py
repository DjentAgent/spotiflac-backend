import asyncio
import base64
import binascii
import json
import logging
import re
import time
import urllib.parse
from typing import List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import aiohttp
import aioredis
from aiohttp.client_exceptions import ContentTypeError
from bs4 import BeautifulSoup
from fastapi import HTTPException
from rapidfuzz import fuzz
import bencodepy

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

try:
    import libtorrent as lt  # type: ignore
except Exception:
    lt = None

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
    orig = v
    if v.lower().startswith("urn:btih:"):
        v = v.split(":", 2)[-1]
    if v.lower().startswith("urn:btmh:"):
        log.debug("BTIH normalize: got BTMH (v2) '%s' -> unsupported for caches", orig)
        return None

    if len(v) == 40 and all(c in "0123456789abcdefABCDEF" for c in v):
        hv = v.upper()
        if hv != orig:
            log.debug("BTIH normalize: HEX lower -> HEX UPPER %s -> %s", orig, hv)
        return hv

    if len(v) == 32:
        try:
            raw = base64.b32decode(v.upper())
            hv = binascii.hexlify(raw).decode("ascii").upper()
            log.debug("BTIH normalize: Base32 -> HEX UPPER %s -> %s", orig, hv)
            return hv
        except Exception as e:
            log.debug("BTIH normalize: Base32 decode failed for '%s': %s", orig, e)
            return None

    log.debug("BTIH normalize: unrecognized format '%s'", orig)
    return None


def _slug(s: str) -> str:
    t = re.sub(r"[^A-Za-z0-9._-]+", "-", s).strip("-")
    return t or "torrent"


def _title_maybe_contains_track(title: str, track: str) -> bool:
    if not track:
        return True
    t = (title or "").lower()
    q = track.lower()
    if q in t:
        return True
    return fuzz.partial_ratio(q, t) >= 85


class PirateBayService:
    def __init__(
        self,
        api_base_url: Optional[str] = None,
        redis_url:    Optional[str] = None,
        max_filelist_concurrency: int = None,
        dht_timeout_sec: int = 90,
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
        self.empty_fl_ttl = int(getattr(settings, "piratebay_empty_filelist_ttl", 60 * 60))  # 1h

        # Глобальные лимиты на filelist-путь
        self._fl_sem = asyncio.Semaphore(
            int(max_filelist_concurrency or getattr(settings, "piratebay_filelist_concurrency", 20))
        )
        self._fl_topn = int(getattr(settings, "piratebay_filelist_topn", 20))  # max кандидатов на глуб.проверку
        self._mirror_per_timeout = float(getattr(settings, "piratebay_mirror_timeout_per", 2.5))  # с
        self._mirror_overall_timeout = float(getattr(settings, "piratebay_mirror_timeout_overall", 6.0))  # с

        # Зеркала кэшей .torrent
        self._mirrors = [
            "https://itorrents.org/torrent/{HEX}.torrent",
            "https://itorrents.org/download/{HEX}.torrent",
            "https://itorrents.org/torrent/{HEX}.torrent?title={SLUG}",
            "https://btcache.me/torrent/{HEX}.torrent",
            "https://btcache.me/torrent/{HEX}",
            "https://torrage.info/torrent/{HEX}.torrent",
        ]

        self._dht_timeout_sec = int(
            getattr(settings, "piratebay_dht_timeout_sec", dht_timeout_sec or 90)
        )

        mirrors = getattr(settings, "piratebay_html_mirrors", None)
        if isinstance(mirrors, (list, tuple)) and mirrors:
            self._html_mirrors: list[str] = [m.rstrip("/") for m in mirrors]
        else:
            self._html_mirrors = [
                "https://thepiratebay.org",
                "https://tpb.party",
                "https://pirateproxy.live",
                "https://thehiddenbay.com",
            ]

        cfg_trackers = getattr(settings, "piratebay_default_trackers", None)
        self._default_trackers: list[str] = (
            list(cfg_trackers) if isinstance(cfg_trackers, (list, tuple)) and cfg_trackers else [
                "udp://tracker.opentrackr.org:1337/announce",
                "udp://open.stealth.si:80/announce",
                "udp://tracker.torrent.eu.org:451/announce",
                "udp://opentracker.i2p.rocks:6969/announce",
                "udp://explodie.org:6969/announce",
                "udp://tracker.internetwarriors.net:1337/announce",
                "udp://tracker1.bt.moack.co.kr:80/announce",
                "udp://tracker-udp.gbitt.info:80/announce",
                "http://tracker.opentrackr.org:1337/announce",
                "http://open.tracker.cl:1337/announce",
                "http://tracker.gbitt.info:80/announce",
                "http://t.nyaatracker.com:80/announce",
                "http://tracker.files.fm:6969/announce",
            ]
        )

        log.info(
            "PirateBayService init: api_base=%s, redis=%s, DHT=%s, mirrors=%s, "
            "default_trackers=%d, dht_timeout=%ds, fl_concurrency=%d",
            self.api_base, r_url, ("enabled" if lt else "disabled"),
            self._mirrors, len(self._default_trackers), self._dht_timeout_sec, self._fl_sem._value
        )

    # ----------------- cache helpers -----------------
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

        # 1) запрос к apibay
        url = f"{self.api_base}/q.php"
        params = {"q": query, "cat": 100}  # Audio
        try:
            async with self.http.get(url, params=params) as resp:
                resp.raise_for_status()
                items = await resp.json()
        except Exception as e:
            log.exception("Search API error: %s", e)
            raise HTTPException(status_code=502, detail="PirateBay API error")

        # 2) чистим «пустышки»
        clean_items = []
        for it in items if isinstance(items, list) else []:
            if not isinstance(it, dict):
                continue
            tid = str(it.get("id", "")).strip()
            name = (it.get("name") or "").strip()
            if not tid.isdigit() or tid == "0" or name.lower().startswith("no results"):
                continue
            clean_items.append(it)

        # 3) базовый lossless/losy фильтр
        filtered = []
        for it in clean_items:
            name = it.get("name", "") or ""
            is_l = bool(_LOSSLESS_RE.search(name))
            is_y = bool(_LOSSY_RE.search(name))
            if only_lossless is True and (not is_l or is_y):
                continue
            if only_lossless is False and is_l:
                continue
            filtered.append(it)

        # 4) ускоренный фильтр по track (title fast-pass + ограниченная глубина filelist)
        if track:
            track_low = track.lower()

            # 4.1 fast-pass по заголовку
            fast_hits, candidates = [], []
            for it in filtered:
                name = it.get("name", "") or ""
                if _title_maybe_contains_track(name, track):
                    fast_hits.append(it)
                else:
                    candidates.append(it)

            # 4.2 формируем пул deep-кандидатов
            def _size_bytes(d: dict) -> int:
                try:
                    return int(d.get("size", 0) or 0)
                except Exception:
                    return 0

            topn_seeders = int(getattr(settings, "piratebay_filelist_topn_seeders", 20))
            topn_size = int(getattr(settings, "piratebay_filelist_topn_size", 10))
            size_floor = int(getattr(settings, "piratebay_filelist_size_floor_bytes", 500 * 1024 * 1024))  # 500 MB
            kw_list = getattr(settings, "piratebay_filelist_title_keywords",
                              ["discog", "complete", "collection", "anthology", "best of", "full"])

            by_seeders = sorted(candidates, key=lambda d: int(d.get("seeders", 0) or 0), reverse=True)[:topn_seeders]
            by_size = sorted(candidates, key=_size_bytes, reverse=True)[:topn_size]
            specials = [it for it in candidates
                        if _size_bytes(it) >= size_floor
                        or any(kw in (it.get("name", "").lower()) for kw in kw_list)]

            # без дублей, с приоритетом «сидеры → размер → спец»
            seen_ids: set[str] = set()
            deep: list[dict] = []
            for bucket in (by_seeders, by_size, specials):
                for it in bucket:
                    tid = str(it.get("id", "")).strip()
                    if tid and tid not in seen_ids:
                        deep.append(it); seen_ids.add(tid)

            if not deep and candidates:
                deep = candidates[: min(len(candidates), max(topn_seeders, 10))]

            async def check(it: dict) -> Optional[dict]:
                tid = str(it["id"])
                async with self._fl_sem:
                    fl = await self._get_filelist(tid)  # берём только через apibay/itorrents, без DHT
                for fname in fl:
                    f_low = fname.lower()
                    if track_low in f_low or fuzz.partial_ratio(track_low, f_low) >= 80:
                        return it
                return None

            results = await asyncio.gather(*(check(it) for it in deep))
            deep_hits = [it for it in results if it]

            # итог: быстрые по заголовку + подтверждённые по списку файлов
            filtered = fast_hits + deep_hits

        # 5) сборка ответа
        out: List[dict] = []
        infos: List[TorrentInfo] = []
        for it in filtered:
            size_bytes = int(it.get("size", 0) or 0)
            page_url = f"https://thepiratebay.org/?t={it['id']}"
            info = TorrentInfo(
                title=it.get("name", "") or "",
                url=page_url,
                size=_human_size(size_bytes),
                seeders=int(it.get("seeders", 0) or 0),
                leechers=int(it.get("leechers", 0) or 0),
            )
            infos.append(info)
            out.append(info.__dict__)

        await self.redis.set(cache_key, json.dumps(out), ex=self.search_ttl)
        return infos

    # ------------------------------ helpers: parse .torrent ------------------------------
    def _parse_torrent_filelist(self, data: bytes) -> List[str]:
        try:
            meta = bencodepy.decode(data)
        except Exception:
            return []
        info = meta.get(b"info") if isinstance(meta, dict) else None
        if not isinstance(info, dict):
            return []
        files = info.get(b"files")
        if isinstance(files, list) and files:
            out: List[str] = []
            for f in files:
                if not isinstance(f, dict):
                    continue
                parts = f.get(b"path")
                if isinstance(parts, list) and parts:
                    try:
                        name = "/".join([p.decode("utf-8", "ignore") for p in parts])
                        if name:
                            out.append(name)
                    except Exception:
                        continue
            if out:
                return out
        name = info.get(b"name")
        if isinstance(name, (bytes, bytearray)):
            s = name.decode("utf-8", "ignore").strip()
            return [s] if s else []
        return []

    # ------------------------------ mirrors race (fast) ------------------------------
    async def _fetch_from_mirrors(self, info_hash_hex: str, title_slug: str) -> Optional[bytes]:
        """
        Параллельно дергаем зеркала с коротким таймаутом на каждое и общим дедлайном.
        Первое валидное — победитель, остальные отменяем.
        """
        async def _one(url: str) -> Optional[bytes]:
            url = url.replace("{HEX}", info_hash_hex).replace("{SLUG}", title_slug)
            try:
                timeout = aiohttp.ClientTimeout(total=self._mirror_per_timeout)
                async with self.http.get(url, allow_redirects=True, timeout=timeout) as r:
                    data = await r.read()
                    ct = (r.headers.get("Content-Type") or "").lower()
                    if r.status == 200 and (ct.startswith("application/x-bittorrent") or data.startswith(b"d8:")):
                        return data
                    if r.status == 200 and ct.startswith("text/html"):
                        text = data.decode("utf-8", errors="ignore")
                        m = re.search(r'href=["\'](https?://[^"\']+\.torrent[^"\']*)["\']', text, re.IGNORECASE)
                        if m:
                            real = m.group(1)
                            async with self.http.get(real, allow_redirects=True, timeout=timeout) as r2:
                                d2 = await r2.read()
                                ct2 = (r2.headers.get("Content-Type") or "").lower()
                                if r2.status == 200 and (ct2.startswith("application/x-bittorrent") or d2.startswith(b"d8:")):
                                    return d2
            except Exception:
                return None
            return None

        tasks = [asyncio.create_task(_one(u)) for u in self._mirrors]
        try:
            done, pending = await asyncio.wait(
                tasks, timeout=self._mirror_overall_timeout, return_when=asyncio.FIRST_COMPLETED
            )
            for d in done:
                data = d.result()
                if data:
                    for p in pending:
                        p.cancel()
                    return data
            if pending:
                done2, pending2 = await asyncio.wait(pending, timeout=max(0.0, self._mirror_overall_timeout / 2))
                for d in done2:
                    data = d.result()
                    if data:
                        for p in pending2:
                            p.cancel()
                        return data
                for p in pending2:
                    p.cancel()
            return None
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()

    # ------------------------------ filelist (NO DHT) ------------------------------
    async def _get_filelist(self, torrent_id: str) -> List[str]:
        cache_key = f"pb:fl:{torrent_id}"
        if cached := await self.redis.get(cache_key):
            try:
                return json.loads(cached)
            except Exception:
                pass
        if await self.redis.get(f"pb:fl_empty:{torrent_id}"):
            return []

        # 1) apibay f.php
        url = f"{self.api_base}/f.php"
        names: List[str] = []
        try:
            async with self.http.get(url, params={"id": torrent_id}) as resp:
                resp.raise_for_status()
                try:
                    files = await resp.json(content_type=None)
                except ContentTypeError:
                    text = await resp.text()
                    files = json.loads(text)
                for f in files:
                    nl = f.get("name")
                    if isinstance(nl, list) and nl:
                        names.append(nl[0])
                if names:
                    await self.redis.set(cache_key, json.dumps(names), ex=self.filelist_ttl)
                    return names
        except Exception:
            pass

        # 2) mirrors race по btih (без DHT)
        info_hash_hex: Optional[str] = None
        title_slug = "torrent"
        try:
            async with self.http.get(f"{self.api_base}/t.php", params={"id": torrent_id}) as r:
                if r.status == 200:
                    meta = await r.json()
                    ih_raw = meta.get("info_hash") or meta.get("infohash") or ""
                    info_hash_hex = _hex_upper_from_btih(ih_raw)
                    nm = (meta.get("name") or "").strip()
                    if nm:
                        title_slug = _slug(nm)
        except Exception:
            pass
        if not info_hash_hex:
            info_hash_hex = await self._resolve_info_hash_from_id(torrent_id)
        if not info_hash_hex:
            await self.redis.set(f"pb:fl_empty:{torrent_id}", "1", ex=self.empty_fl_ttl)
            return []

        blob_key = f"pb:blob:{info_hash_hex}"
        data = await self._cache_get_blob(blob_key)
        if not data:
            data = await self._fetch_from_mirrors(info_hash_hex, title_slug)
            if data:
                await self._cache_set_blob(blob_key, data, ex=min(self.torrent_ttl, 24*3600))
        if not data:
            await self.redis.set(f"pb:fl_empty:{torrent_id}", "1", ex=self.empty_fl_ttl)
            return []

        fl = self._parse_torrent_filelist(data)
        if fl:
            await self.redis.set(cache_key, json.dumps(fl), ex=self.filelist_ttl)
        else:
            await self.redis.set(f"pb:fl_empty:{torrent_id}", "1", ex=self.empty_fl_ttl)
        return fl

    # ------------------------------ magnet/tracker helpers ------------------------------
    async def _magnet_from_page_bs4(self, torrent_id: str) -> Tuple[Optional[str], list[str]]:
        paths = [f"/description.php?id={torrent_id}", f"/?t={torrent_id}"]
        for base in self._html_mirrors:
            for path in paths:
                url = f"{base}{path}"
                try:
                    async with self.http.get(url) as resp:
                        if resp.status != 200:
                            continue
                        html = await resp.text()
                except Exception:
                    continue
                try:
                    soup = BeautifulSoup(html, "lxml")
                except Exception:
                    soup = BeautifulSoup(html, "html.parser")
                a = soup.find("a", href=True, attrs={"href": re.compile(r"^magnet:\?")})
                if not a:
                    continue
                magnet = a["href"]
                try:
                    parsed = urllib.parse.urlparse(magnet)
                    qs = urllib.parse.parse_qs(parsed.query)
                except Exception:
                    continue
                btih_hex: Optional[str] = None
                for xt in qs.get("xt", []):
                    if xt.lower().startswith("urn:btih:"):
                        btih_hex = _hex_upper_from_btih(xt)
                        break
                if not btih_hex:
                    continue
                trackers = []
                for tr in qs.get("tr", []):
                    try:
                        tr_dec = urllib.parse.unquote_plus(tr)
                        if tr_dec:
                            trackers.append(tr_dec)
                    except Exception:
                        pass
                return btih_hex, trackers
        return None, []

    async def _magnet_from_page(self, torrent_id: str) -> Tuple[Optional[str], list[str]]:
        for url in (
            f"https://thepiratebay.org/?t={torrent_id}",
            f"https://thepiratebay.org/description.php?id={torrent_id}",
        ):
            try:
                async with self.http.get(url) as resp:
                    if resp.status != 200:
                        continue
                    html = await resp.text()
            except Exception:
                continue
            m = re.search(r'href=["\'](magnet:\?[^"\']+)', html, re.IGNORECASE)
            if not m:
                continue
            magnet = m.group(1)
            parsed = urllib.parse.urlparse(magnet)
            qs = urllib.parse.parse_qs(parsed.query)
            btih = None
            for xt in qs.get("xt", []):
                if xt.lower().startswith("urn:btih:"):
                    btih = _hex_upper_from_btih(xt)
                    break
            trackers = [urllib.parse.unquote_plus(t) for t in qs.get("tr", [])]
            return btih, trackers
        return None, []

    # ------------------------------ harvest trackers from cache pages ------------------------------
    async def _harvest_trackers_for_hash(self, info_hash_hex: str) -> list[str]:
        """
        Собираем трекеры со страниц HTML-зеркал кэшей (часто содержат полноценный magnet с tr=).
        """
        urls = [
            f"https://btcache.me/torrent/{info_hash_hex}",
            f"https://itorrents.org/torrent/{info_hash_hex}.torrent",     # иногда HTML
            f"https://itorrents.org/download/{info_hash_hex}.torrent",    # иногда HTML
        ]
        trackers: list[str] = []
        seen = set()

        def _extract_from_html(html: str) -> None:
            for m in re.finditer(r'href=["\'](magnet:\?[^"\']+)["\']', html, re.IGNORECASE):
                magnet = m.group(1)
                try:
                    parsed = urllib.parse.urlparse(magnet)
                    qs = urllib.parse.parse_qs(parsed.query)
                    for tr in qs.get("tr", []):
                        tr_dec = urllib.parse.unquote_plus(tr).strip()
                        if tr_dec and tr_dec not in seen:
                            trackers.append(tr_dec); seen.add(tr_dec)
                except Exception:
                    continue

        for url in urls:
            try:
                timeout = aiohttp.ClientTimeout(total=min(self._mirror_per_timeout, 3.0))
                async with self.http.get(url, allow_redirects=True, timeout=timeout) as r:
                    if r.status != 200:
                        continue
                    ct = (r.headers.get("Content-Type") or "").lower()
                    if "text/html" in ct or "text/plain" in ct:
                        text = await r.text(errors="ignore")
                        _extract_from_html(text)
                    else:
                        _ = await r.read()
            except Exception:
                continue

        return trackers

    # ------------------------------ id -> info_hash ------------------------------
    async def _resolve_info_hash_from_id(self, torrent_id: str) -> Optional[str]:
        cache_key = f"pb:id2hash:{torrent_id}"
        if cached := await self.redis.get(cache_key):
            ih = (cached or "").strip()
            if ih:
                return ih
        url = f"{self.api_base}/t.php"
        try:
            async with self.http.get(url, params={"id": torrent_id}) as resp:
                resp.raise_for_status()
                meta = await resp.json()
                ih_raw = meta.get("info_hash") or meta.get("infohash") or ""
                ih_norm = _hex_upper_from_btih(ih_raw)
                if ih_norm:
                    await self.redis.set(cache_key, ih_norm, ex=self.id2hash_ttl)
                    return ih_norm
        except Exception:
            pass
        ih_fb, _ = await self._magnet_from_page_bs4(torrent_id)
        if not ih_fb:
            ih_fb, _ = await self._magnet_from_page(torrent_id)
        if ih_fb:
            await self.redis.set(cache_key, ih_fb, ex=self.id2hash_ttl)
            return ih_fb
        return None

    async def _btih_from_page(self, page_url: str) -> Optional[str]:
        try:
            async with self.http.get(page_url) as resp:
                status = resp.status
                html = await resp.text() if status == 200 else ""
                if status != 200:
                    return None
        except Exception:
            return None
        m = re.search(r'href=["\']magnet:\?[^"\']*xt=urn:btih:([0-9A-Za-z]{32,40})', html, re.IGNORECASE)
        if not m:
            return None
        return _hex_upper_from_btih(m.group(1))

    def _extract_btih_from_url(self, url: str) -> Optional[str]:
        if url.startswith("magnet:"):
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)
            for xt in qs.get("xt", []):
                if xt.lower().startswith("urn:btih:"):
                    return _hex_upper_from_btih(xt)
                if xt.lower().startswith("urn:btmh:"):
                    return None
            return None
        return None

    # ------------------------------ DHT fallback (download only) ------------------------------
    def _apply_lt_settings(self, ses):
        try:
            sp = lt.settings_pack()
            try:
                sp.set_int(lt.settings_pack.alert_mask, lt.alert.category_t.all_categories)
            except Exception:
                pass
            for attr, val in [
                (getattr(lt.settings_pack, "announce_to_all_trackers", None), True),
                (getattr(lt.settings_pack, "announce_to_all_tiers", None), True),
                (getattr(lt.settings_pack, "enable_outgoing_utp", None), True),
                (getattr(lt.settings_pack, "enable_incoming_utp", None), True),
                (getattr(lt.settings_pack, "enable_outgoing_tcp", None), True),
                (getattr(lt.settings_pack, "enable_incoming_tcp", None), True),
            ]:
                if attr is not None:
                    sp.set_bool(attr, val)
            try:
                sp.set_str(getattr(lt.settings_pack, "listen_interfaces"), "0.0.0.0:6881,[::]:6881")
            except Exception:
                pass
            ses.apply_settings(sp)
        except Exception:
            pass

    def _dht_fetch_torrent_blocking(self, info_hash_hex: str, timeout_sec: int, trackers: list[str]) -> Optional[bytes]:
        if lt is None:
            return None
        trackers_all: list[str] = []
        seen = set()
        for tr in (trackers or []) + self._default_trackers:
            tr = (tr or "").strip()
            if tr and tr not in seen:
                trackers_all.append(tr)
                seen.add(tr)
        ses = lt.session()
        try:
            self._apply_lt_settings(ses)
            try: ses.listen_on(6881, 6891)
            except Exception: pass
            for host, port in [
                ("router.bittorrent.com", 6881),
                ("router.utorrent.com", 6881),
                ("dht.transmissionbt.com", 6881),
                ("dht.aelitis.com", 6881),
            ]:
                try: ses.add_dht_router(host, port)
                except Exception: pass
            try: ses.start_dht()
            except Exception: pass
            uri = f"magnet:?xt=urn:btih:{info_hash_hex}"
            params = lt.parse_magnet_uri(uri)
            try: params.save_path = "."
            except Exception:
                try: params["save_path"] = "."
                except Exception: pass
            try:
                if hasattr(params, "trackers") and isinstance(params.trackers, list):
                    params.trackers.extend(trackers_all)
                elif isinstance(params, dict):
                    params.setdefault("trackers", [])
                    params["trackers"].extend(trackers_all)
            except Exception:
                pass
            h = ses.add_torrent(params)
            for tr in trackers_all:
                try: h.add_tracker(tr)
                except Exception: pass
            for _ in range(2):
                try: h.force_reannounce(1)
                except Exception: pass
                try: h.force_dht_announce()
                except Exception: pass
            start = time.time()
            while time.time() - start < max(10, timeout_sec) and not h.has_metadata():
                time.sleep(0.3)
            if not h.has_metadata():
                return None
            ti = h.get_torrent_info()
            try: ct = lt.create_torrent(ti)
            except Exception:
                try: ct = ti.create_torrent()  # type: ignore
                except Exception: ct = lt.create_torrent(ti)
            for tr in trackers_all:
                try: ct.add_tracker(tr)
                except Exception: pass
            try:
                torrent_dict = ct.generate()
                data = lt.bencode(torrent_dict)
            except Exception:
                try: data = lt.bencode(ct.generate())
                except Exception: return None
            return bytes(data)
        except Exception:
            return None
        finally:
            try:
                ses.pause(); ses.stop_dht()
            except Exception:
                pass

    async def _dht_fetch_torrent(self, info_hash_hex: str, timeout_sec: int, trackers: list[str]) -> Optional[bytes]:
        return await asyncio.to_thread(self._dht_fetch_torrent_blocking, info_hash_hex, timeout_sec, trackers)

    # ------------------------------ download ------------------------------
    async def download(self, info: TorrentInfo) -> bytes:
        trackers: list[str] = []

        if info.url.startswith("magnet:"):
            parsed = urlparse(info.url)
            qs = parse_qs(parsed.query)
            for tr in qs.get("tr", []):
                trackers.append(urllib.parse.unquote_plus(tr))
            info_hash_hex = self._extract_btih_from_url(info.url)
        else:
            info_hash_hex = None

        if not info_hash_hex:
            parsed = urlparse(info.url)
            qs = parse_qs(parsed.query)
            tid = None
            for key in ("t", "id"):
                if key in qs and qs[key]:
                    tid = qs[key][0]
                    break
            if tid:
                btih_bs4, trks_bs4 = await self._magnet_from_page_bs4(tid)
                if btih_bs4:
                    info_hash_hex = btih_bs4
                else:
                    info_hash_hex = await self._resolve_info_hash_from_id(tid)
                    if not info_hash_hex:
                        for u in (f"https://thepiratebay.org/?t={tid}",
                                  f"https://thepiratebay.org/description.php?id={tid}"):
                            info_hash_hex = await self._btih_from_page(u)
                            if info_hash_hex:
                                break
                if not trackers:
                    if trks_bs4:
                        trackers.extend(trks_bs4)
                    else:
                        _, trks_old = await self._magnet_from_page_bs4(tid)
                        trackers.extend(trks_old)

        if not info_hash_hex:
            raise HTTPException(
                status_code=422,
                detail="Cannot resolve BTIH (v1). Torrent may be BitTorrent v2 (btmh) or missing."
            )

        cache_key = f"pb:blob:{info_hash_hex}"
        if cached := await self._cache_get_blob(cache_key):
            return cached

        title_slug = _slug(info.title)
        data = await self._fetch_from_mirrors(info_hash_hex, title_slug)
        if data:
            await self._cache_set_blob(cache_key, data, ex=self.torrent_ttl)
            return data

        # ---- NEW: дособираем трекеры со страниц зеркал и объединяем с имеющимися ----
        try:
            extra_trks = await self._harvest_trackers_for_hash(info_hash_hex)
            if extra_trks:
                merged: list[str] = []
                seen = set()
                for tr in trackers + extra_trks + self._default_trackers:
                    tr = (tr or "").strip()
                    if tr and tr not in seen:
                        merged.append(tr); seen.add(tr)
                trackers = merged
                log.debug("download(): harvested %d extra trackers; total=%d", len(extra_trks), len(trackers))
        except Exception:
            pass

        dht_data = await self._dht_fetch_torrent(info_hash_hex, self._dht_timeout_sec, trackers)
        if dht_data:
            await self._cache_set_blob(cache_key, dht_data, ex=self.torrent_ttl)
            return dht_data

        detail = "Torrent not found in public caches"
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