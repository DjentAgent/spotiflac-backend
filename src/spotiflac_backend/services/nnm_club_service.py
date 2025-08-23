"""
Asynchronous service for interacting with the NNM‑Club torrent tracker.

This module provides a class, :class:`NnmClubService`, that exposes high‑level
operations to search for torrents and download `.torrent` files from
nnmclub.to.  The implementation is intentionally similar in spirit to the
existing RuTracker and PirateBay services in this codebase: it performs
unauthenticated HTTP requests to scrape publicly available data, caches
responses in Redis, and exposes simple, coroutine‑friendly methods.

**Key features**
=================

* **Search**: `search()` accepts a query string and returns a list of
  ``TorrentInfo`` objects.  Under the hood it makes a GET request to
  ``/forum/tracker.php?nm=<query>`` on nnmclub.to and parses the HTML
  table of results.  Each result includes the torrent title, human
  readable size, number of seeders and leechers, and a URL that can be
  passed back into the service for downloading.  Optionally you can
  filter results to only include "lossless" formats or those that
  contain a particular track name by examining the file list inside
  the torrent.

* **Download**: `download_by_id()` takes an integer torrent identifier and
  returns the raw bytes of the corresponding `.torrent` file.  For
  public (so‑called “platinum”) releases this endpoint does not
  require authentication; if you attempt to fetch a private torrent
  without being logged in then NNM‑Club will redirect to the login
  page and this method will raise an exception.

* **File list parsing**: for more sophisticated filtering, the service
  downloads the `.torrent` file and decodes it using `bencodepy` to
  extract the list of filenames.  The results are cached to avoid
  repeatedly hitting the tracker or decoding large bencoded blobs.

* **Caching**: a Redis connection can be supplied via ``redis_url``.  If
  present, the service will cache search results, file lists and
  downloaded torrents using sensible TTLs.  If omitted, the service
  still functions but without persistence.

The service does *not* handle authentication.  NNM‑Club employs a
Cloudflare Turnstile on its login form which cannot be solved via
simple HTTP requests.  If you need to access private torrents you
should provide your own session cookies or use a logged in session
retrieved through another mechanism.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import urllib.parse
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union, Any

import aiohttp
import bencodepy
from redis.asyncio import Redis

# Attempt to import the project's TorrentInfo model if present.  If not,
# fall back to a simple dataclass so this module remains self‑contained.
try:
    from spotiflac_backend.models.torrent import TorrentInfo  # type: ignore
except Exception:

    @dataclass
    class TorrentInfo:
        """A minimal representation of a torrent search result.

        Attributes
        ----------
        title: str
            Human readable title of the torrent (e.g. "Megadeth – Rust in Peace").
        url: str
            Link back to the torrent on nnmclub.to.  This is typically the
            ``download.php?id=...`` URL which can be passed into
            :meth:`download_by_id`.
        size: str
            Human readable size, such as "264 MB".
        seeders: int
            Number of seeders.
        leechers: int
            Number of leechers.
        """

        title: str
        url: str
        size: str
        seeders: int
        leechers: int


class NnmClubService:
    """High‑level API for searching and downloading torrents from NNM‑Club.

    Parameters
    ----------
    api_base : str
        Base URL for the tracker.  Defaults to ``https://nnmclub.to/forum``.
    redis_url : Optional[str]
        If provided, an async Redis client will be created and used to
        cache search results, file lists and downloaded torrents.  If
        omitted, caching is disabled.
    search_ttl : int
        How long (in seconds) to cache search results.  Defaults to
        five minutes.
    filelist_ttl : int
        How long (in seconds) to cache decoded file lists.  Defaults to
        one day.
    torrent_ttl : int
        How long (in seconds) to cache raw torrent blobs.  Defaults to
        one week.

    Notes
    -----
    NNM‑Club exposes a simple HTTP interface for searching and
    downloading torrents.  Most endpoints are publicly accessible for
    "platinum" releases, so you rarely need to authenticate.  If you
    attempt to download a private torrent without being logged in the
    service will raise a :class:`ValueError`.
    """

    def __init__(
        self,
        api_base: str = "https://nnmclub.to/forum",
        *,
        redis_url: Optional[str] = None,
        search_ttl: int = 5 * 60,
        filelist_ttl: int = 24 * 3600,
        torrent_ttl: int = 7 * 24 * 3600,
    ) -> None:
        self.log = logging.getLogger(self.__class__.__name__)
        self.api_base = api_base.rstrip("/")
        self.search_ttl = search_ttl
        self.filelist_ttl = filelist_ttl
        self.torrent_ttl = torrent_ttl

        # Create an aiohttp session with sane defaults.  Use a
        # reasonable timeout to avoid hanging forever on slow responses.
        self.http = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20),
            headers={
                # Present a common browser user‑agent to avoid bot
                # detection heuristics.
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/114.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
        )

        # Optional Redis client for caching.  If redis_url is None
        # caching methods become no‑ops.
        self.redis: Optional[Redis] = None
        if redis_url:
            # Intentionally let exceptions propagate on misconfiguration:
            # the calling application should handle connection errors.
            self.redis = Redis.from_url(redis_url, encoding="utf-8", decode_responses=False)

    # ----------------------------------------------------------------------
    # Internal helpers for caching
    # ----------------------------------------------------------------------
    async def _cache_get(self, key: str) -> Optional[Union[bytes, str]]:
        if not self.redis:
            return None
        try:
            return await self.redis.get(key)  # type: ignore[no-any-return]
        except Exception as exc:
            self.log.debug("Redis get failed for %s: %s", key, exc)
            return None

    async def _cache_set(self, key: str, value: Union[bytes, str], ex: int) -> None:
        if not self.redis:
            return
        try:
            await self.redis.set(key, value, ex=ex)
        except Exception as exc:
            self.log.debug("Redis set failed for %s: %s", key, exc)

    # ----------------------------------------------------------------------
    # Search API
    # ----------------------------------------------------------------------
    async def search(
        self,
        query: str,
        *,
        only_lossless: Optional[bool] = None,
        track: Optional[str] = None,
    ) -> List[TorrentInfo]:
        """Search for torrents on NNM‑Club and return a list of results.

        Parameters
        ----------
        query : str
            Keywords to search for.  Only a single search term is
            supported; NNM‑Club does not provide advanced boolean search
            syntax for unauthenticated queries.
        only_lossless : Optional[bool], default ``None``
            If ``True``, return only those results whose title contains
            evidence of a lossless format (e.g. FLAC, APE, ALAC).  If
            ``False``, filter out clearly lossless results and prefer lossy
            formats.  If ``None``, return all.
        track : Optional[str], default ``None``
            If provided, attempt to filter results down to torrents that
            include this exact track name in their file list.  This
            requires downloading and decoding the `.torrent` file for
            each candidate and will be slower.

        Returns
        -------
        List[TorrentInfo]
            A list of search results.  The order matches the ordering on
            the tracker (newer or more popular torrents first).
        """
        # Build a cache key.  We normalise the query to lower case and
        # include the optional parameters so distinct calls don't clash.
        query_key = query.strip().lower()
        cache_key = f"nnm:search:{query_key}:{only_lossless}:{track}"
        cached = await self._cache_get(cache_key)
        if cached:
            try:
                data = json.loads(cached)  # type: ignore[arg-type]
                return [TorrentInfo(**d) for d in data]  # type: ignore[call-arg]
            except Exception as exc:
                self.log.debug("Failed to decode cached search for %s: %s", query, exc)

        params = {"nm": query}
        url = f"{self.api_base}/tracker.php"
        async with self.http.get(url, params=params) as resp:
            resp.raise_for_status()
            text = await resp.text(encoding="windows-1251", errors="ignore")

        results: List[Tuple[TorrentInfo, str]] = []
        for info, torrent_id in self._parse_search_page(text):
            # Apply optional lossless/lossy filtering on title.
            lowered = info.title.lower()
            is_lossless = bool(re.search(r"\b(flac|ape|alac|lossless|wavpack|wv|tta)\b", lowered))
            if only_lossless is True and not is_lossless:
                continue
            if only_lossless is False and is_lossless:
                continue
            # If a track filter is provided, fetch the file list and
            # perform a fuzzy search for the track within it.
            if track:
                try:
                    fl = await self._get_filelist(torrent_id)
                except Exception:
                    fl = []
                lowered_track = track.lower()
                match = any(
                    lowered_track in name.lower() or self._ratio(lowered_track, name.lower()) >= 80
                    for name in fl
                )
                if not match:
                    continue
            results.append((info, torrent_id))

        # Drop the torrent_id when returning.  We keep it during
        # filtering so we don't repeatedly decode the same torrent file.
        final_results = [info for info, _ in results]
        if self.redis and final_results:
            try:
                # Serialise dataclasses/pydantic models as dicts.
                out_json = json.dumps([info.__dict__ for info in final_results], ensure_ascii=False)
                await self._cache_set(cache_key, out_json, ex=self.search_ttl)
            except Exception as exc:
                self.log.debug("Failed to cache search results for %s: %s", query, exc)

        return final_results

    def _parse_search_page(self, html: str) -> List[Tuple[TorrentInfo, str]]:
        """Parse the HTML returned from the tracker search page.

        The search result table has the following columns: forum category,
        topic title, author, a ``[ DL ]`` cell with a link to
        ``download.php?id=...``, raw size (bytes) and human readable
        size, number of seeders and leechers, ratio and a timestamp.

        Returns
        -------
        List[Tuple[TorrentInfo, str]]
            Each entry contains the parsed ``TorrentInfo`` and the
            corresponding numeric identifier extracted from the download
            link.  The identifier is used later to fetch the file list.
        """
        results: List[Tuple[TorrentInfo, str]] = []
        # We walk through the HTML and look for occurrences of
        # ``download.php?id=`` which mark each result row.  Each row
        # contains multiple pieces of data separated by HTML tags.  A
        # simple regular expression is sufficient and avoids pulling in
        # BeautifulSoup for this one task.
        row_re = re.compile(
            r'<a href="download\.php\?id=(?P<id>\d+)"[^>]*>\s*\[\s*<b>DL</b>\s*\]</a>'
            r'.*?<u>(?P<size_bytes>\d+)</u>\s+(?P<size_human>[\d.,]+\s*\w+)'  # raw size and human size
            r'.*?<b>(?P<seeders>\d+)</b>.*?<b>(?P<leechers>\d+)</b>',
            re.IGNORECASE | re.DOTALL
        )
        title_re = re.compile(
            r'<a href="(?P<url>[^"]+)"[^>]*>\s*(?P<title>[^<]+?)\s*</a>'
        )

        # Use an iterator over matches so we can capture the surrounding
        # text for the title.  We'll work backwards: for each DL link we
        # scan backwards in the HTML to find the preceding title link.
        for match in row_re.finditer(html):
            torrent_id = match.group('id')
            size_human = match.group('size_human').strip()
            seeders = int(match.group('seeders'))
            leechers = int(match.group('leechers'))
            # Heuristic: the topic link appears earlier in the same row
            # before the DL link.  We search backwards a bit from the
            # start of the DL link to find the last anchor that links to
            # a topic (viewtopic.php).  If not found, we skip this
            # result.
            start = match.start()
            window = html[max(0, start - 2000):start]
            title_match = None
            for m in reversed(list(title_re.finditer(window))):
                if 'viewtopic.php' in m.group('url'):
                    title_match = m
                    break
            if not title_match:
                continue
            title = title_match.group('title').strip()
            # The download link we return to callers should be the
            # ``download.php?id=...`` URL because it's directly usable.
            download_url = urllib.parse.urljoin(self.api_base + '/', f"download.php?id={torrent_id}")
            info = TorrentInfo(title=title, url=download_url, size=size_human, seeders=seeders, leechers=leechers)
            results.append((info, torrent_id))

        return results

    # ----------------------------------------------------------------------
    # File list extraction
    # ----------------------------------------------------------------------
    async def _get_filelist(self, torrent_id: str) -> List[str]:
        """Return a list of file names contained in the specified torrent.

        This method downloads the `.torrent` file from NNM‑Club and
        decodes its contents using the bencodepy library.  Results are
        cached in Redis if a cache is configured.

        Parameters
        ----------
        torrent_id : str
            The numeric identifier from the search results (i.e. the
            portion after ``download.php?id=``).

        Returns
        -------
        List[str]
            A list of UTF‑8 decoded file paths contained within the
            torrent.  For single file torrents the list contains one
            entry.

        Raises
        ------
        ValueError
            If the `.torrent` file cannot be downloaded or decoded.
        """
        cache_key = f"nnm:filelist:{torrent_id}"
        cached = await self._cache_get(cache_key)
        if cached:
            try:
                return json.loads(cached)  # type: ignore[no-any-return]
            except Exception:
                pass

        torrent_data = await self._download_torrent(torrent_id)
        try:
            meta = bencodepy.decode(torrent_data)
        except Exception as exc:
            raise ValueError(f"Failed to decode torrent {torrent_id}: {exc}") from exc
        info = meta.get(b"info") if isinstance(meta, dict) else None
        if not isinstance(info, dict):
            raise ValueError(f"Invalid torrent meta for {torrent_id}")
        file_list: List[str] = []
        # Multi file torrent
        if b"files" in info:
            files = info[b"files"]
            if isinstance(files, list):
                for f in files:
                    if not isinstance(f, dict):
                        continue
                    path = f.get(b"path")
                    if isinstance(path, list):
                        try:
                            file_name = "/".join(p.decode("utf-8", "ignore") for p in path)
                            file_list.append(file_name)
                        except Exception:
                            continue
        # Single file torrent
        else:
            name = info.get(b"name")
            if isinstance(name, (bytes, bytearray)):
                try:
                    file_list.append(name.decode("utf-8", "ignore"))
                except Exception:
                    pass

        if self.redis and file_list:
            try:
                await self._cache_set(cache_key, json.dumps(file_list, ensure_ascii=False), ex=self.filelist_ttl)
            except Exception as exc:
                self.log.debug("Failed to cache file list for %s: %s", torrent_id, exc)
        return file_list

    # ----------------------------------------------------------------------
    # Download API
    # ----------------------------------------------------------------------
    async def _download_torrent(self, torrent_id: str) -> bytes:
        """Fetch the raw `.torrent` file for the given ID.

        The caller is responsible for ensuring the torrent is public; if
        the tracker redirects to the login page a :class:`ValueError`
        will be raised.
        """
        cache_key = f"nnm:blob:{torrent_id}"
        cached = await self._cache_get(cache_key)
        if cached:
            # cached value is stored as base64 encoded string because Redis
            # cannot reliably store binary blobs without encoding.
            try:
                return base64.b64decode(cached)  # type: ignore[arg-type]
            except Exception:
                pass

        url = urllib.parse.urljoin(self.api_base + '/', f"download.php?id={torrent_id}")
        async with self.http.get(url) as resp:
            # If the torrent is private and we are not logged in the
            # server will return a 302 redirect to the login page.  In
            # that case raise an error.
            if resp.status in {301, 302}:
                raise ValueError(f"Torrent {torrent_id} requires authentication")
            resp.raise_for_status()
            data = await resp.read()

        if self.redis and data:
            try:
                encoded = base64.b64encode(data).decode('ascii')
                await self._cache_set(cache_key, encoded, ex=self.torrent_ttl)
            except Exception as exc:
                self.log.debug("Failed to cache torrent %s: %s", torrent_id, exc)
        return data

    async def download_by_id(self, torrent_id: Union[str, int]) -> bytes:
        """Public method to download a torrent given its numeric ID."""
        return await self._download_torrent(str(torrent_id))

    async def download(self, info: TorrentInfo) -> bytes:
        """Download the `.torrent` file for the given search result.

        This convenience wrapper accepts a :class:`TorrentInfo` and
        extracts the ID from its URL.
        """
        # Extract the id from a URL like ``.../download.php?id=1234``.  If
        # parsing fails raise a ValueError.
        parsed = urllib.parse.urlparse(info.url)
        qs = urllib.parse.parse_qs(parsed.query)
        torrent_id = None
        for key in ("id", "t"):
            if key in qs and qs[key]:
                torrent_id = qs[key][0]
                break
        if not torrent_id:
            raise ValueError(f"Cannot extract torrent id from {info.url}")
        return await self._download_torrent(torrent_id)

    # ----------------------------------------------------------------------
    # Utility: fuzzy ratio
    # ----------------------------------------------------------------------
    @staticmethod
    def _ratio(a: str, b: str) -> int:
        """Compute a simple fuzzy match score between two strings.

        This utility replicates a tiny subset of `rapidfuzz.partial_ratio` to
        avoid an additional dependency.  It returns a value between 0 and
        100 representing how similar the strings are.
        """
        # Normalize whitespace and case
        a = re.sub(r"\s+", " ", a).strip().lower()
        b = re.sub(r"\s+", " ", b).strip().lower()
        if not a or not b:
            return 0
        if a == b:
            return 100
        # Ensure a is the shorter
        if len(a) > len(b):
            a, b = b, a
        max_score = 0
        for i in range(len(b) - len(a) + 1):
            window = b[i : i + len(a)]
            matches = sum(ch1 == ch2 for ch1, ch2 in zip(a, window))
            score = matches * 100 // len(a)
            if score > max_score:
                max_score = score
        return max_score

    # ----------------------------------------------------------------------
    async def close(self) -> None:
        """Gracefully shut down the HTTP and Redis clients."""
        try:
            await self.http.close()
        except Exception:
            pass
        if self.redis:
            try:
                await self.redis.aclose()
            except Exception:
                pass