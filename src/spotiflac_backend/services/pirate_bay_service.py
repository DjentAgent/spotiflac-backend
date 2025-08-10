import asyncio
import base64
import binascii
import json
import logging
import re
import time
import urllib.parse
from typing import List, Optional
from urllib.parse import urlparse, parse_qs

import aiohttp
import aioredis
from aiohttp.client_exceptions import ContentTypeError
from bs4 import BeautifulSoup
from fastapi import HTTPException
from rapidfuzz import fuzz

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

# libtorrent (опционально для DHT-фолбэка)
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

    # HEX v1
    if len(v) == 40 and all(c in "0123456789abcdefABCDEF" for c in v):
        hv = v.upper()
        if hv != orig:
            log.debug("BTIH normalize: HEX lower -> HEX UPPER %s -> %s", orig, hv)
        return hv

    # Base32 v1
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


class PirateBayService:
    def __init__(
        self,
        api_base_url: Optional[str] = None,
        redis_url:    Optional[str] = None,
        max_filelist_concurrency: int = 10,
        # базовый таймаут DHT/TR-фолбэка (переопределяется settings.piratebay_dht_timeout_sec)
        dht_timeout_sec: int = 60,
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

        # таймаут можно переопределить через настройки (по умолчанию 120)
        self._dht_timeout_sec = int(
            getattr(settings, "piratebay_dht_timeout_sec", dht_timeout_sec or 60)
        )

        # HTML-зеркала TPB для парсинга магнита/трекеров (на случай 403/вариаций страницы)
        mirrors = getattr(settings, "piratebay_html_mirrors", None)
        if isinstance(mirrors, (list, tuple)) and mirrors:
            self._html_mirrors: list[str] = [m.rstrip("/") for m in mirrors]
        else:
            self._html_mirrors = [
                "https://thepiratebay.org",
                "https://tpb.party",
            ]

        # дефолтные публичные трекеры (сливаются с теми, что придут из magnet)
        cfg_trackers = getattr(settings, "piratebay_default_trackers", None)
        self._default_trackers: list[str] = (
            list(cfg_trackers) if isinstance(cfg_trackers, (list, tuple)) and cfg_trackers else [
                "udp://tracker.opentrackr.org:1337/announce",
                "udp://open.stealth.si:80/announce",
                "udp://tracker.torrent.eu.org:451/announce",
                "udp://tracker-udp.gbitt.info:80/announce",
                "udp://opentracker.i2p.rocks:6969/announce",
                "udp://explodie.org:6969/announce",
                "udp://tracker.internetwarriors.net:1337/announce",
                "udp://tracker1.bt.moack.co.kr:80/announce",
                "http://tracker.opentrackr.org:1337/announce",
                "http://open.tracker.cl:1337/announce",
            ]
        )

        log.info(
            "PirateBayService init: api_base=%s, redis=%s, DHT=%s, mirrors=%s, "
            "default_trackers=%d, dht_timeout=%ds",
            self.api_base, r_url, ("enabled" if lt else "disabled (libtorrent not installed)"),
            self._mirrors, len(self._default_trackers), self._dht_timeout_sec
        )

    # ----------------- cache helpers (torrent blob) -----------------
    async def _cache_get_blob(self, key: str) -> Optional[bytes]:
        s = await self.redis.get(key)
        if not s:
            log.debug("Redis blob miss: %s", key)
            return None
        try:
            data = base64.b64decode(s)
            log.debug("Redis blob hit: %s (size=%d)", key, len(data))
            return data
        except Exception:
            log.exception("Failed to b64-decode blob cache for %s", key)
            return None

    async def _cache_set_blob(self, key: str, data: bytes, ex: int) -> None:
        b64 = base64.b64encode(data).decode("ascii")
        await self.redis.set(key, b64, ex=ex)
        log.debug("Redis blob set: %s (size=%d, ttl=%d)", key, len(data), ex)

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
                log.debug("Search cache hit: %s -> %d items", cache_key, len(data))
                return [TorrentInfo(**d) for d in data]
            except Exception:
                log.exception("Failed to load search cache %s", cache_key)

        url = f"{self.api_base}/q.php"
        params = {"q": query, "cat": 100}  # Audio
        log.debug("Search request -> %s params=%s", url, params)
        try:
            async with self.http.get(url, params=params) as resp:
                resp.raise_for_status()
                items = await resp.json()
                log.debug("Search response: %d raw items", len(items))
        except Exception as e:
            log.exception("Search API error: %s", e)
            raise HTTPException(status_code=502, detail="PirateBay API error")

        # --- ВАЖНО: убираем «пустышку» от apibay ---
        clean_items = []
        skipped_dummy = 0
        for it in items if isinstance(items, list) else []:
            if not isinstance(it, dict):
                continue
            tid = str(it.get("id", "")).strip()
            name = (it.get("name") or "").strip()
            # Отбрасываем id == "0" и/или имя "No results returned"
            if not tid.isdigit() or tid == "0" or name.lower().startswith("no results"):
                skipped_dummy += 1
                continue
            clean_items.append(it)
        if skipped_dummy:
            log.debug("Search: dropped %d dummy 'No results returned' items", skipped_dummy)

        before = len(clean_items)
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
        log.debug("Search filtered by lossless=%s: %d -> %d", only_lossless, before, len(filtered))

        if track:
            track_low = track.lower()
            log.debug("Track filter enabled: '%s'", track)

            async def check(it: dict) -> Optional[dict]:
                async with self._fl_sem:
                    fl = await self._get_filelist(str(it["id"]))
                for fname in fl:
                    f_low = fname.lower()
                    if track_low in f_low or fuzz.partial_ratio(track_low, f_low) >= 80:
                        return it
                return None

            results = await asyncio.gather(*(check(it) for it in filtered))
            n = sum(1 for x in results if x)
            filtered = [it for it in results if it]
            log.debug("Track filter matched: %d", n)

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
        log.debug("Search cache set: %s (items=%d, ttl=%d)", cache_key, len(out), self.search_ttl)
        return infos

    # ------------------------------ filelist ------------------------------
    async def _get_filelist(self, torrent_id: str) -> List[str]:
        cache_key = f"pb:fl:{torrent_id}"
        if cached := await self.redis.get(cache_key):
            try:
                lst = json.loads(cached)
                log.debug("Filelist cache hit for id=%s: %d files", torrent_id, len(lst))
                return lst
            except Exception:
                log.exception("Failed to load filelist cache %s", cache_key)

        url = f"{self.api_base}/f.php"
        log.debug("Filelist request -> %s?id=%s", url, torrent_id)
        try:
            async with self.http.get(url, params={"id": torrent_id}) as resp:
                resp.raise_for_status()
                try:
                    files = await resp.json(content_type=None)
                except ContentTypeError:
                    text = await resp.text()
                    files = json.loads(text)
                log.debug("Filelist response for id=%s: %d entries", torrent_id, len(files))
        except (ContentTypeError, json.JSONDecodeError) as e:
            log.exception("Filelist parse error (id=%s): %s", torrent_id, e)
            raise HTTPException(status_code=502, detail="PirateBay filelist parse error")
        except Exception as e:
            log.exception("Filelist API error (id=%s): %s", torrent_id, e)
            raise HTTPException(status_code=502, detail="PirateBay filelist error")

        names: List[str] = []
        for f in files:
            nl = f.get("name")
            if isinstance(nl, list) and nl:
                names.append(nl[0])

        await self.redis.set(cache_key, json.dumps(names), ex=self.filelist_ttl)
        log.debug("Filelist cache set for id=%s: %d files, ttl=%d", torrent_id, len(names), self.filelist_ttl)
        return names

    # ------------------------------ magnet/tracker helpers ------------------------------
    async def _magnet_from_page_bs4(self, torrent_id: str) -> tuple[Optional[str], list[str]]:
        """
        Пытаемся получить (btih_hex, trackers[]) c нескольких HTML-зеркал TPB.
        Сначала пробуем /description.php?id=, затем /?t=.
        """
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

                log.debug(
                    "BS4 magnet parse OK from %s: btih=%s, trackers=%d",
                    url, btih_hex, len(trackers)
                )
                return btih_hex, trackers

        log.debug("BS4 magnet parse failed for id=%s on all mirrors", torrent_id)
        return None, []

    async def _magnet_from_page(self, torrent_id: str) -> tuple[Optional[str], list[str]]:
        """Регекс-фолбэк: (btih_hex, trackers) с основной страницы TPB."""
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

    # ------------------------------ info_hash resolve ------------------------------
    async def _resolve_info_hash_from_id(self, torrent_id: str) -> Optional[str]:
        cache_key = f"pb:id2hash:{torrent_id}"
        if cached := await self.redis.get(cache_key):
            ih = (cached or "").strip()
            if ih:
                log.debug("id2hash cache hit: id=%s -> %s", torrent_id, ih)
                return ih

        # 1) apibay t.php
        url = f"{self.api_base}/t.php"
        log.debug("id2hash: querying %s?id=%s", url, torrent_id)
        try:
            async with self.http.get(url, params={"id": torrent_id}) as resp:
                resp.raise_for_status()
                meta = await resp.json()
                ih_raw = meta.get("info_hash") or meta.get("infohash") or ""
                ih_norm = _hex_upper_from_btih(ih_raw)
                log.debug("id2hash: t.php returned info_hash=%s -> normalized=%s", ih_raw, ih_norm)
                if ih_norm:
                    await self.redis.set(cache_key, ih_norm, ex=self.id2hash_ttl)
                    return ih_norm
        except Exception as e:
            log.warning("id2hash: t.php error for id=%s: %s", torrent_id, e)

        # 2) HTML: берём magnet и вытаскиваем btih
        ih_fb, _ = await self._magnet_from_page(torrent_id)
        log.debug("id2hash: magnet_from_page -> btih=%s", ih_fb)
        if ih_fb:
            await self.redis.set(cache_key, ih_fb, ex=self.id2hash_ttl)
            return ih_fb

        log.debug("id2hash: failed to resolve id=%s", torrent_id)
        return None

    async def _btih_from_page(self, page_url: str) -> Optional[str]:
        try:
            async with self.http.get(page_url) as resp:
                status = resp.status
                html = await resp.text() if status == 200 else ""
                log.debug("Fetch TPB page: %s (status=%d, len=%d)", page_url, status, len(html))
                if status != 200:
                    return None
        except Exception as e:
            log.warning("TPB page fetch failed for %s: %s", page_url, e)
            return None

        m = re.search(
            r'href=["\']magnet:\?[^"\']*xt=urn:btih:([0-9A-Za-z]{32,40})',
            html, re.IGNORECASE
        )
        if not m:
            log.debug("TPB page parse: magnet not found on %s", page_url)
            return None
        btih = m.group(1)
        norm = _hex_upper_from_btih(btih)
        log.debug("TPB page parse: btih=%s -> normalized=%s", btih, norm)
        return norm

    def _extract_btih_from_url(self, url: str) -> Optional[str]:
        if url.startswith("magnet:"):
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)
            for xt in qs.get("xt", []):
                if xt.lower().startswith("urn:btih:"):
                    ih = _hex_upper_from_btih(xt)
                    log.debug("extract_btih_from_url: magnet btih=%s -> %s", xt, ih)
                    if ih:
                        return ih
                if xt.lower().startswith("urn:btmh:"):
                    log.debug("extract_btih_from_url: magnet btmh found -> unsupported for caches")
                    return None
            return None
        return None

    # ------------------------------ DHT+Trackers fallback ------------------------------
    def _dht_fetch_torrent_blocking(self, info_hash_hex: str, timeout_sec: int, trackers: list[str]) -> Optional[bytes]:
        if lt is None:
            log.info("DHT fallback skipped: libtorrent not installed")
            return None

        # объединяем трекеры: из magnet + дефолтные, с удалением дублей и пустых
        trackers_all: list[str] = []
        seen = set()
        for tr in (trackers or []) + self._default_trackers:
            if tr and tr not in seen:
                trackers_all.append(tr)
                seen.add(tr)

        log.info(
            "DHT/TR fallback: starting for %s (timeout=%ds, trackers=%d)",
            info_hash_hex, timeout_sec, len(trackers_all)
        )
        log.debug("DHT/TR trackers list: %s", trackers_all)

        ses = lt.session()
        try:
            # применяем settings_pack: все категории алёртов, агрессивные анонсы, TCP/uTP
            try:
                sp = lt.settings_pack()
                # полный alert mask (tracker_announce/reply/error и пр.)
                try:
                    sp.set_int(lt.settings_pack.alert_mask, lt.alert.category_t.all_categories)
                except Exception:
                    pass
                # агрессивнее объявляться
                for flag, val in [
                    (getattr(lt.settings_pack, "announce_to_all_trackers", None), True),
                    (getattr(lt.settings_pack, "announce_to_all_tiers", None), True),
                    (getattr(lt.settings_pack, "enable_outgoing_utp", None), True),
                    (getattr(lt.settings_pack, "enable_incoming_utp", None), True),
                    (getattr(lt.settings_pack, "enable_outgoing_tcp", None), True),
                    (getattr(lt.settings_pack, "enable_incoming_tcp", None), True),
                ]:
                    if flag is not None:
                        sp.set_bool(flag, val)
                try:
                    sp.set_str(getattr(lt.settings_pack, "listen_interfaces"), "0.0.0.0:6881,[::]:6881")
                except Exception:
                    pass
                ses.apply_settings(sp)
            except Exception as e:
                log.debug("lt settings_pack apply warning: %s", e)

            ses.listen_on(6881, 6891)
            try:
                ses.add_dht_router("router.bittorrent.com", 6881)
                ses.add_dht_router("router.utorrent.com", 6881)
                ses.add_dht_router("dht.transmissionbt.com", 6881)
                ses.add_dht_router("dht.aelitis.com", 6881)
            except Exception as e:
                log.debug("DHT: add_dht_router warning: %s", e)
            ses.start_dht()

            uri = f"magnet:?xt=urn:btih:{info_hash_hex}"
            params = lt.parse_magnet_uri(uri)
            params.save_path = "."
            if hasattr(params, "trackers") and isinstance(params.trackers, list):
                params.trackers.extend(trackers_all)

            h = ses.add_torrent(params)
            # ускоряем: объявляемся в трекеры и DHT
            try:
                h.force_reannounce(0)
            except Exception:
                pass
            try:
                h.force_dht_announce()
            except Exception:
                pass

            start = time.time()
            next_log = start
            next_reannounce = start + 15.0
            while time.time() - start < max(10, timeout_sec) and not h.has_metadata():
                time.sleep(0.3)
                # алёрты libtorrent
                alerts = ses.pop_alerts()
                for a in alerts:
                    what = getattr(a, "what", lambda: a.__class__.__name__)()
                    msg  = getattr(a, "message", lambda: str(a))()
                    if "error" in what.lower() or "failed" in what.lower():
                        log.warning("lt-alert: %s — %s", what, msg)
                    else:
                        log.debug("lt-alert: %s — %s", what, msg)

                now = time.time()
                if now - next_log >= 5:
                    st = h.status()
                    log.debug(
                        "DHT/TR: waiting metadata... peers=%d, down=%.1fKB/s, up=%.1fKB/s, elapsed=%.1fs",
                        st.num_peers, st.download_rate / 1024, st.upload_rate / 1024, now - start
                    )
                    next_log = now
                # периодический форс-реанонс в трекеры
                if now >= next_reannounce:
                    try:
                        h.force_reannounce(0)
                    except Exception:
                        pass
                    next_reannounce = now + 15.0

            if not h.has_metadata():
                log.info("DHT/TR fallback: timeout without metadata for %s", info_hash_hex)
                return None

            ti = h.get_torrent_info()
            ct = lt.create_torrent(ti)
            # переносим трекеры в финальный .torrent
            for tr in trackers_all:
                try:
                    ct.add_tracker(tr)
                except Exception:
                    pass

            torrent_dict = ct.generate()
            data = lt.bencode(torrent_dict)
            log.info("DHT/TR fallback: success for %s (size=%d bytes, elapsed=%.2fs)",
                     info_hash_hex, len(data), time.time() - start)
            return bytes(data)
        except Exception as e:
            log.exception("DHT/TR fallback: unexpected error for %s: %s", info_hash_hex, e)
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
        log.debug("download(): title=%s url=%s", info.title, info.url)

        trackers: list[str] = []

        # 1) magnet → btih + trackers
        if info.url.startswith("magnet:"):
            parsed = urlparse(info.url)
            qs = parse_qs(parsed.query)
            for tr in qs.get("tr", []):
                trackers.append(urllib.parse.unquote_plus(tr))
            info_hash_hex = self._extract_btih_from_url(info.url)
        else:
            info_hash_hex = None

        # 2) ?t=/id → btih (+ трекеры со страницы)
        if not info_hash_hex:
            parsed = urlparse(info.url)
            qs = parse_qs(parsed.query)
            tid = None
            for key in ("t", "id"):
                if key in qs and qs[key]:
                    tid = qs[key][0]
                    break
            log.debug("download(): extracted tid=%s from url", tid)
            if tid:
                # сначала пробуем надёжный BS4-парсер магнит/трекеров с зеркал
                btih_bs4, trks_bs4 = await self._magnet_from_page_bs4(tid)
                if btih_bs4:
                    info_hash_hex = btih_bs4
                else:
                    # если не удалось — стандартный путь: t.php → regex-фолбэк
                    info_hash_hex = await self._resolve_info_hash_from_id(tid)
                    if not info_hash_hex:
                        for u in (f"https://thepiratebay.org/?t={tid}",
                                  f"https://thepiratebay.org/description.php?id={tid}"):
                            info_hash_hex = await self._btih_from_page(u)
                            if info_hash_hex:
                                break
                # трекеры: берём из BS4, если есть; иначе пытаемся достать со страницы
                if not trackers:
                    if trks_bs4:
                        trackers.extend(trks_bs4)
                    else:
                        _, trks_old = await self._magnet_from_page_bs4(tid)
                        trackers.extend(trks_old)

        if not info_hash_hex:
            log.info("download(): cannot resolve BTIH for url=%s", info.url)
            raise HTTPException(
                status_code=422,
                detail="Cannot resolve BTIH (v1). Torrent may be BitTorrent v2 (btmh) or missing."
            )

        cache_key = f"pb:blob:{info_hash_hex}"
        if cached := await self._cache_get_blob(cache_key):
            log.info("download(): served from Redis blob cache, btih=%s size=%d", info_hash_hex, len(cached))
            return cached

        # 3) пробуем кеш-зеркала
        last_exc: Optional[Exception] = None
        for tmpl in self._mirrors:
            url = tmpl.replace("{HEX}", info_hash_hex)
            try:
                log.debug("download(): trying mirror %s", url)
                async with self.http.get(url) as r:
                    data = await r.read()
                    ct = r.headers.get("Content-Type", "")
                    log.debug("download(): mirror status=%d ct=%s len=%d", r.status, ct, len(data))
                    if r.status == 200 and (ct.startswith("application/x-bittorrent") or data.startswith(b"d")):
                        await self._cache_set_blob(cache_key, data, ex=self.torrent_ttl)
                        log.info("download(): mirror hit %s (btih=%s size=%d)", url, info_hash_hex, len(data))
                        return data
            except Exception as e:
                log.debug("download(): mirror error %s: %s", url, e)
                last_exc = e
                continue

        # 4) DHT+Trackers fallback
        dht_data = await self._dht_fetch_torrent(info_hash_hex, self._dht_timeout_sec, trackers)
        if dht_data:
            await self._cache_set_blob(cache_key, dht_data, ex=self.torrent_ttl)
            log.info("download(): returned DHT/TR-synthesized .torrent for btih=%s", info_hash_hex)
            return dht_data

        detail = "Torrent not found in public caches"
        if last_exc is not None:
            detail += f" (last error: {last_exc})"
        if lt is None:
            detail += "; DHT fallback unavailable (libtorrent not installed)"
        log.info("download(): NOT FOUND for btih=%s -> %s", info_hash_hex, detail)
        raise HTTPException(status_code=404, detail=detail)

    async def download_by_id(self, torrent_id: str) -> bytes:
        log.debug("download_by_id(): id=%s", torrent_id)
        ih = await self._resolve_info_hash_from_id(torrent_id)
        if not ih:
            log.info("download_by_id(): no info_hash for id=%s", torrent_id)
            raise HTTPException(status_code=404, detail="Metadata not found or unsupported (btmh/v2)")
        ti = TorrentInfo(title="", url=f"https://thepiratebay.org/?t={torrent_id}",
                         size="0 B", seeders=0, leechers=0)
        return await self.download(ti)

    async def download_by_hash(self, info_hash: str) -> bytes:
        log.debug("download_by_hash(): raw=%s", info_hash)
        ih = _hex_upper_from_btih(info_hash)
        if not ih:
            log.info("download_by_hash(): invalid BTIH %s", info_hash)
            raise HTTPException(status_code=422, detail="Invalid BTIH (expect 40-char hex or 32-char base32)")
        ti = TorrentInfo(title="", url=f"magnet:?xt=urn:btih:{ih}",
                         size="0 B", seeders=0, leechers=0)
        return await self.download(ti)

    async def close(self) -> None:
        await self.http.close()
        await self.redis.close()
        log.debug("PirateBayService closed HTTP+Redis")

    def __del__(self):
        if getattr(self, "http", None) and not self.http.closed:
            loop = asyncio.get_event_loop()
            loop.create_task(self.http.close())