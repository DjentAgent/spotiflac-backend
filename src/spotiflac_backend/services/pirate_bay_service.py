"""
Service for searching and downloading torrents from The Pirate Bay via the
unofficial ``apibay.org`` API.  The service exposes an interface similar to
``RutrackerService``: it allows searching for torrents with optional
lossless/lossy filtering and track name filtering, and it supports
downloading the corresponding ``.torrent`` file.

The search API endpoints used here are documented in the unofficial
Pirate Bay API: ``q.php`` returns search results, ``t.php`` returns
metadata for a single torrent, and ``f.php`` returns the list of files in
the torrent.  For downloading the actual ``.torrent`` file, we rely on
public torrent caches that host ``.torrent`` files by info hash (e.g.
``itorrents.org`` or ``torrage.info``).

This service uses Redis for caching search results, file lists and torrent
blobs to reduce load on external services.  The TTL values and base URL
can be configured via ``settings`` from ``spotiflac_backend.core.config``.

Usage:
    service = PirateBayService()
    results = await service.search("artist album", only_lossless=True)
    torrent_bytes = await service.download(results[0])

"""

import json
import logging
import re
import urllib.parse
from typing import List, Optional, Sequence

import requests
from fastapi import HTTPException
from rapidfuzz import fuzz

try:
    import redis  # type: ignore
except ImportError as exc:  # pragma: no cover - redis is optional for tests
    redis = None  # type: ignore

try:
    import cloudscraper  # type: ignore
except ImportError:
    cloudscraper = None  # type: ignore

# Import project settings and models.  These imports will succeed when the
# service is used within the spotiflac_backend project.  They are kept
# optional so that the file can be type‑checked standalone.
try:
    from spotiflac_backend.core.config import settings  # type: ignore
    from spotiflac_backend.models.torrent import TorrentInfo  # type: ignore
except Exception:  # pragma: no cover - standalone usage
    # Fallback stubs for standalone testing.  If you run this file directly,
    # define a minimal ``settings`` object and a ``TorrentInfo`` dataclass
    # yourself.
    class SettingsStub:
        piratebay_api_base: str = "https://apibay.org"
        redis_url: str = "redis://localhost:6379/0"
        piratebay_search_ttl: int = 5 * 60
        piratebay_filelist_ttl: int = 24 * 3600
        piratebay_torrent_ttl: int = 7 * 24 * 3600

    settings = SettingsStub()  # type: ignore

    from dataclasses import dataclass
    @dataclass
    class TorrentInfo:  # type: ignore
        title: str
        url: str
        size: str
        seeders: int
        leechers: int

log = logging.getLogger(__name__)

# Regular expressions used to detect lossless and lossy audio keywords.
_LOSSLESS_RE = re.compile(
    r"\b(flac|wavpack|wv|ape|alac|aiff|pcm|dts|mlp|tta|mqa|lossless)\b",
    re.IGNORECASE,
)
_LOSSY_RE = re.compile(
    r"\b(mp3|aac|ogg|opus|lossy)\b",
    re.IGNORECASE,
)


def _human_size(nbytes: int) -> str:
    """Convert an integer number of bytes into a human‑readable string.

    :param nbytes: Size in bytes
    :return: e.g. ``"1.23 GB"`` or ``"456 MB"``
    """
    suffixes = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(nbytes)
    for suffix in suffixes:
        if value < 1024.0:
            return f"{value:.2f} {suffix}"
        value /= 1024.0
    return f"{value:.2f} EB"


class PirateBayService:
    """Service providing search and download capabilities for Pirate Bay."""

    def __init__(self, api_base_url: Optional[str] = None) -> None:
        # Determine the API base URL.  We default to the value from settings
        # (``settings.piratebay_api_base``) or fall back to "https://apibay.org".
        base = getattr(settings, "piratebay_api_base", None) or "https://apibay.org"
        self.api_base_url: str = (api_base_url or base).rstrip("/")

        # Create an HTTP session with Cloudflare support if possible.  If
        # ``cloudscraper`` is unavailable, fall back to ``requests.Session()``.
        if cloudscraper is not None:
            try:
                self.scraper = cloudscraper.create_scraper(
                    browser={"browser": "chrome", "platform": "windows", "mobile": False}
                )
            except Exception:
                # If cloudscraper setup fails, degrade gracefully to requests.Session
                self.scraper = requests.Session()
        else:
            self.scraper = requests.Session()

        # Set a sensible default timeout for all requests made via this session.
        self.scraper.timeout = 20  # type: ignore[attr-defined]

        # Initialise Redis client for caching.  If ``redis`` is not available
        # (e.g. in a standalone environment), set ``self.redis`` to ``None``.
        if redis is not None:
            self.redis = redis.Redis.from_url(getattr(settings, "redis_url", "redis://localhost:6379/0"))
        else:
            self.redis = None  # type: ignore

        # TTLs for caching search results, file lists and torrent blobs (in seconds).
        self.search_ttl: int = int(getattr(settings, "piratebay_search_ttl", 5 * 60))
        self.filelist_ttl: int = int(getattr(settings, "piratebay_filelist_ttl", 24 * 3600))
        self.torrent_ttl: int = int(getattr(settings, "piratebay_torrent_ttl", 7 * 24 * 3600))

    # ---------------------------------------------------------------------
    # Search
    #
    async def search(
        self,
        query: str,
        only_lossless: Optional[bool] = None,
        track: Optional[str] = None,
    ) -> List[TorrentInfo]:
        """Search Pirate Bay for torrents matching ``query``.

        :param query: Search terms
        :param only_lossless: If ``True``, return only lossless torrents; if
            ``False``, return only lossy; if ``None``, return both.
        :param track: Optional track title to filter results by file name.
        :return: List of ``TorrentInfo`` objects
        """
        import asyncio
        return await asyncio.to_thread(self._search_sync, query, only_lossless, track)

    def _search_sync(
        self,
        query: str,
        only_lossless: Optional[bool],
        track: Optional[str],
    ) -> List[TorrentInfo]:
        # Compose a cache key for this search
        cache_key = f"piratebay:search:{query}:{only_lossless}:{track}"
        # Return cached results if available
        if self.redis is not None:
            cached = self.redis.get(cache_key)
            if cached:
                try:
                    raw_list = json.loads(cached)
                    return [TorrentInfo(**item) for item in raw_list]  # type: ignore[arg-type]
                except Exception:
                    log.exception("Failed to deserialize search cache for %s", cache_key)
                    # Ignore cache on deserialization errors
                    pass

        # Perform the search via the API
        url = f"{self.api_base_url}/q.php"
        params = {"q": query, "cat": 0}
        try:
            resp = self.scraper.get(url, params=params)
            resp.raise_for_status()
            items = resp.json()
        except Exception as e:
            log.exception("Error searching Pirate Bay: %s", e)
            raise HTTPException(status_code=502, detail="Failed to query Pirate Bay API") from e

        results: List[TorrentInfo] = []
        # Preprocess track search term
        track_lower = track.lower() if track else None

        for item in items:
            # Ensure required fields are present
            try:
                tid = str(item["id"])
                title = str(item["name"])
                info_hash = str(item["info_hash"])
                se = int(item.get("seeders", 0))
                le = int(item.get("leechers", 0))
                size_bytes = int(item.get("size", 0))
            except Exception:
                # Skip invalid items
                continue

            # Apply lossless/lossy filter based on the title (and later file names)
            is_l = bool(_LOSSLESS_RE.search(title))
            is_y = bool(_LOSSY_RE.search(title))
            if only_lossless is True and (not is_l or is_y):
                continue
            if only_lossless is False and is_l:
                continue

            # If track name filter is provided, ensure the track appears in the
            # file list.  We use a separate helper that handles caching.
            if track_lower:
                try:
                    filelist = self._get_filelist(tid)
                except HTTPException as http_exc:
                    # If the API returns 404 or other errors, skip this item
                    log.debug("Skipping tid %s due to filelist error: %s", tid, http_exc.detail)
                    continue
                # Check if track name matches any file name.  Use fuzzy
                # matching similar to RutrackerService: accept if either
                # substring match or partial ratio >= 80.
                matched = False
                for fname in filelist:
                    f_low = fname.lower()
                    if track_lower in f_low or fuzz.partial_ratio(track_lower, f_low) >= 80:
                        matched = True
                        break
                if not matched:
                    continue

            # Build a pseudo download URL containing the torrent ID so that clients
            # can extract the ID and pass it to our download endpoint.  This
            # mirrors the format used by RuTracker (``dl.php?t=<id>``) but
            # points at Pirate Bay.  The actual link does not need to be
            # functional; it simply carries the ID.
            pseudo_url = f"https://thepiratebay.org/?t={tid}"

            results.append(
                TorrentInfo(
                    title=title,
                    url=pseudo_url,
                    size=_human_size(size_bytes),
                    seeders=se,
                    leechers=le,
                )
            )

        # Cache the results in Redis
        if self.redis is not None:
            try:
                # Serialize using json so that we don't depend on pydantic
                serialized = [
                    {
                        "title": r.title,
                        "url": r.url,
                        "size": r.size,
                        "seeders": r.seeders,
                        "leechers": r.leechers,
                    }
                    for r in results
                ]
                self.redis.set(cache_key, json.dumps(serialized), ex=self.search_ttl)
            except Exception:
                log.exception("Failed to cache search results for %s", cache_key)

        return results

    # ---------------------------------------------------------------------
    # File list retrieval
    #
    def _get_filelist(self, torrent_id: str) -> Sequence[str]:
        """Retrieve and cache the list of file names for a Pirate Bay torrent.

        :param torrent_id: ID of the torrent from ``apibay`` search results
        :return: List of file names
        :raises HTTPException: If the API returns an error or invalid data
        """
        cache_key = f"piratebay:filelist:{torrent_id}"
        if self.redis is not None:
            cached = self.redis.get(cache_key)
            if cached:
                try:
                    return json.loads(cached)
                except Exception:
                    log.exception("Failed to deserialize filelist cache for %s", torrent_id)
                    pass

        url = f"{self.api_base_url}/f.php"
        params = {"id": torrent_id}
        try:
            resp = self.scraper.get(url, params=params)
            resp.raise_for_status()
            files = resp.json()
            # The API returns a list of objects with ``name`` and ``size`` arrays
            file_names: List[str] = []
            for f in files:
                # Each entry has a ``name`` field which is a list with a single
                # file name; take the first element
                name_list = f.get("name")
                if name_list and isinstance(name_list, list):
                    try:
                        file_names.append(str(name_list[0]))
                    except Exception:
                        continue
            # Cache the file list
            if self.redis is not None:
                try:
                    self.redis.set(cache_key, json.dumps(file_names), ex=self.filelist_ttl)
                except Exception:
                    log.exception("Failed to cache filelist for %s", torrent_id)
            return file_names
        except Exception as e:
            # Translate errors into HTTPException
            raise HTTPException(status_code=502, detail=f"Failed to fetch file list: {e}") from e

    # ---------------------------------------------------------------------
    # Download .torrent
    #
    async def download(self, info: TorrentInfo) -> bytes:
        """Download the ``.torrent`` file for the given search result.

        :param info: A ``TorrentInfo`` object from this service's search results.
        :return: Raw bytes of the ``.torrent`` file
        :raises HTTPException: If the file cannot be fetched
        """
        import asyncio
        return await asyncio.to_thread(self._download_sync, info)

    def _download_sync(self, info: TorrentInfo) -> bytes:
        # Extract the info_hash from the magnet link.  We parse the query
        # parameters and locate the ``xt=urn:btih:<hash>`` entry.
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(info.url)
        qs = parse_qs(parsed.query)
        xt_list = qs.get("xt") or []
        # xt may be present as ``urn:btih:<hash>``; extract the hash part
        info_hash = None
        for xt in xt_list:
            if xt.startswith("urn:btih:"):
                info_hash = xt.split(":")[-1].upper()
                break
        if not info_hash:
            raise HTTPException(status_code=400, detail="Invalid magnet link: no info hash")

        cache_key = f"piratebay:blob:{info_hash}"
        if self.redis is not None:
            cached = self.redis.get(cache_key)
            if cached:
                return cached

        # List of mirrors that host .torrent files by info hash.  We try them
        # sequentially until one succeeds.
        mirrors = [
            f"https://itorrents.org/torrent/{info_hash}.torrent",
            f"https://torrage.info/torrent/{info_hash}.torrent",
            f"https://btcache.me/torrent/{info_hash}",
        ]

        last_error: Optional[Exception] = None
        for url in mirrors:
            try:
                log.debug("Attempting to download torrent from %s", url)
                r = self.scraper.get(url, timeout=30)
                # If the response is successful and looks like a torrent file,
                # return it
                if r.status_code == 200 and (
                    "application/x-bittorrent" in r.headers.get("Content-Type", "")
                    or r.content.startswith(b"d8:announce")
                ):
                    torrent_data = r.content
                    # Cache the torrent blob
                    if self.redis is not None:
                        try:
                            self.redis.set(cache_key, torrent_data, ex=self.torrent_ttl)
                        except Exception:
                            log.exception("Failed to cache torrent blob for %s", info_hash)
                    return torrent_data
                elif r.status_code == 404:
                    continue  # Try next mirror
            except Exception as e:
                last_error = e
                continue

        # If no mirror succeeded, raise an error
        message = "Torrent file not found"
        if last_error:
            message = f"Torrent file not found: {last_error}"
        raise HTTPException(status_code=404, detail=message)

    # ------------------------------------------------------------------
    # Direct download by ID or info hash
    #
    async def download_by_id(self, torrent_id: str) -> bytes:
        """Download a torrent by its Pirate Bay ID.

        This method looks up the torrent metadata via the ``t.php`` API, extracts
        the info hash and delegates to :meth:`download_by_hash`.

        :param torrent_id: The ``id`` returned by the search API
        :return: Raw bytes of the ``.torrent`` file
        :raises HTTPException: On API errors or if the torrent cannot be fetched
        """
        import asyncio
        return await asyncio.to_thread(self._download_by_id_sync, torrent_id)

    def _download_by_id_sync(self, torrent_id: str) -> bytes:
        # Retrieve torrent metadata from the t.php endpoint
        url = f"{self.api_base_url}/t.php"
        try:
            resp = self.scraper.get(url, params={"id": torrent_id})
            resp.raise_for_status()
            meta = resp.json()
            info_hash = meta.get("info_hash")
            if not info_hash:
                raise HTTPException(status_code=404, detail="Torrent metadata not found")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Failed to fetch torrent metadata: {e}") from e
        # Delegate to download by hash
        return self._download_by_hash_sync(str(info_hash))

    async def download_by_hash(self, info_hash: str) -> bytes:
        """Download a torrent directly by its info hash.

        :param info_hash: 40‑character hexadecimal info hash
        :return: Raw bytes of the ``.torrent`` file
        """
        import asyncio
        return await asyncio.to_thread(self._download_by_hash_sync, info_hash)

    def _download_by_hash_sync(self, info_hash: str) -> bytes:
        # Normalize the hash
        info_hash = info_hash.upper()
        # Check cache
        cache_key = f"piratebay:blob:{info_hash}"
        if self.redis is not None:
            cached = self.redis.get(cache_key)
            if cached:
                return cached
        # List of mirrors as in _download_sync
        mirrors = [
            f"https://itorrents.org/torrent/{info_hash}.torrent",
            f"https://torrage.info/torrent/{info_hash}.torrent",
            f"https://btcache.me/torrent/{info_hash}",
        ]
        last_error: Optional[Exception] = None
        for url in mirrors:
            try:
                r = self.scraper.get(url, timeout=30)
                if r.status_code == 200 and (
                    "application/x-bittorrent" in r.headers.get("Content-Type", "")
                    or r.content.startswith(b"d8:announce")
                ):
                    torrent_data = r.content
                    if self.redis is not None:
                        try:
                            self.redis.set(cache_key, torrent_data, ex=self.torrent_ttl)
                        except Exception:
                            log.exception("Failed to cache torrent blob for %s", info_hash)
                    return torrent_data
                elif r.status_code == 404:
                    continue
            except Exception as e:
                last_error = e
                continue
        message = "Torrent file not found"
        if last_error:
            message = f"Torrent file not found: {last_error}"
        raise HTTPException(status_code=404, detail=message)