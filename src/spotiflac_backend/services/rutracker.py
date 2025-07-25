import json
import logging
import re
import uuid
import os
import time
from concurrent.futures import ThreadPoolExecutor
from math import ceil
from typing import List, Optional, Tuple
from urllib.parse import urljoin, parse_qs

import bencodepy
import cloudscraper
import lxml.html
import redis

from fastapi import HTTPException
from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

log = logging.getLogger(__name__)

# Регулярные выражения
_LOSSLESS_RE    = re.compile(r"\b(flac|wavpack|wv|ape|alac|aiff|pcm|dts|mlp|tta|mqa|lossless)\b", re.IGNORECASE)
_LOSSY_RE       = re.compile(r"\b(mp3|aac|ogg|opus|lossy)\b", re.IGNORECASE)
_PG_BASE_URL_RE = re.compile(r"PG_BASE_URL\s*:\s*['\"][^?]+\?([^'\"]+)['\"]", re.IGNORECASE)
_TOTAL_RE       = re.compile(r"Результатов поиска:\s*(\d+)", re.IGNORECASE)
_FORM_TOKEN_RE  = re.compile(r"form_token\s*:\s*'([0-9a-f]+)'", re.IGNORECASE)

class CaptchaRequired(Exception):
    def __init__(self, session_id: str, img_url: str):
        super().__init__(f"Captcha required: {session_id}")
        self.session_id = session_id
        self.img_url = img_url

class RutrackerService:
    def __init__(self, base_url: Optional[str] = None):
        self.base_url     = (base_url or settings.rutracker_base).rstrip("/")
        self.scraper      = cloudscraper.create_scraper(
            browser={"browser":"chrome","platform":"windows","mobile":False},
            delay=10
        )
        self.redis        = redis.Redis.from_url(settings.redis_url)
        self.cookie_ttl   = getattr(settings, "rutracker_cookie_ttl",   24*3600)
        self.filelist_ttl = getattr(settings, "rutracker_filelist_ttl", 24*3600)
        self.blob_ttl     = getattr(settings, "rutracker_blob_ttl",     7*24*3600)
        self.search_ttl   = getattr(settings, "rutracker_search_ttl",   5*60)
        self.dump_dir     = getattr(settings, "debug_html_dir", "/app/debug_html")
        self._last_html: Optional[str] = None

        log.debug("Init with base_url=%s", self.base_url)
        try:
            raw = self.redis.get("rutracker:cookiejar")
            if raw:
                cookies = json.loads(raw)
                self.scraper.cookies.update(cookies)
                log.debug("Restored cookies bb_session=%s", cookies.get("bb_session"))
        except Exception:
            log.exception("Cookie restore failed")

    def _extract_form_token(self, html: str) -> str:
        m = _FORM_TOKEN_RE.search(html)
        return m.group(1) if m else ""

    def _login_sync(self):
        login_url = f"{self.base_url}/forum/login.php"
        resp = self.scraper.get(login_url); resp.raise_for_status()
        html = resp.text; self._last_html = html

        try:
            doc = lxml.html.fromstring(html)
            form = doc.get_element_by_id("login-form-full")
        except Exception as e:
            os.makedirs(self.dump_dir, exist_ok=True)
            dump = os.path.join(self.dump_dir, f"login_error_{int(time.time())}.html")
            with open(dump, "w", encoding="utf-8") as f: f.write(html)
            log.error("Login parsing failed, dumped HTML to %s", dump, exc_info=e)
            raise RuntimeError(f"Login parsing failed — HTML dumped to {dump}") from e

        data = {
            inp.get("name"): inp.get("value","")
            for inp in form.xpath(".//input[@type='hidden']") if inp.get("name")
        }
        if token := self._extract_form_token(html):
            data["form_token"] = token
        data.update({
            "login_username": settings.rutracker_login,
            "login_password": settings.rutracker_password,
            "login": form.xpath(".//input[@type='submit']/@value")[0],
        })

        post = self.scraper.post(login_url, data=data, headers={"Referer": login_url}, allow_redirects=True)
        post.raise_for_status()

        jar = self.scraper.cookies.get_dict()
        if not jar.get("bb_session"):
            # CAPTCHA required
            cap = self.scraper.get(login_url); cap.raise_for_status()
            cap_html = cap.text; self._last_html = cap_html
            cdoc = lxml.html.fromstring(cap_html)
            hidden = {
                inp.get("name"): inp.get("value","")
                for inp in cdoc.xpath(".//input[@type='hidden']") if inp.get("name")
            }
            sid  = hidden.get("cap_sid", uuid.uuid4().hex)
            imgs = cdoc.xpath("//img[contains(@src,'/captcha/')]/@src")
            img_url = urljoin(self.base_url+"/forum/", imgs[0]) if imgs else login_url

            code_fields = cdoc.xpath(".//input[starts-with(@name,'cap_code_')]/@name")
            if not code_fields:
                dump = os.path.join(self.dump_dir, f"captcha_parse_error_{sid}_{int(time.time())}.html")
                os.makedirs(self.dump_dir, exist_ok=True)
                with open(dump, "w", encoding="utf-8") as f: f.write(cap_html)
                log.error("Captcha parse failed, dumped HTML to %s", dump)
                raise RuntimeError(f"Captcha parse failed — HTML dumped to {dump}")
            code_field = code_fields[0]
            submit_val = cdoc.xpath(".//input[@type='submit']/@value")[0]

            self.redis.set(f"login:{sid}", json.dumps({
                "hidden":     hidden,
                "code_field": code_field,
                "submit_val": submit_val,
            }), ex=300)
            raise CaptchaRequired(sid, img_url)

        # «Прогрев» сессии
        self.scraper.get(f"{self.base_url}/forum/tracker.php").raise_for_status()

        self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)
        log.debug("Login succeeded, bb_session=%s", jar.get("bb_session"))

    def _ensure_login(self):
        if not self.scraper.cookies.get("bb_session"):
            self._login_sync()

    def initiate_login(self) -> Tuple[Optional[str], Optional[str]]:
        login_url = f"{self.base_url}/forum/login.php"
        resp = self.scraper.get(login_url); resp.raise_for_status()
        doc  = lxml.html.fromstring(resp.text)
        form = doc.get_element_by_id("login-form-full")
        hidden = {
            inp.get("name"): inp.get("value","")
            for inp in form.xpath(".//input[@type='hidden']") if inp.get("name")
        }
        sid  = hidden.get("cap_sid")
        imgs = doc.xpath("//img[contains(@src,'/captcha/')]/@src")
        if not sid or not imgs:
            return None, None

        code_field = form.xpath(".//input[starts-with(@name,'cap_code_')]/@name")[0]
        submit_val = form.xpath(".//input[@type='submit']/@value")[0]
        session_id = uuid.uuid4().hex
        self.redis.set(f"login:{session_id}", json.dumps({
            "hidden":     hidden,
            "code_field": code_field,
            "submit_val": submit_val
        }), ex=300)
        img_url = urljoin(self.base_url+"/forum/", imgs[0])
        raise CaptchaRequired(session_id, img_url)

    def complete_login(self, session_id: str, solution: str):
        raw = self.redis.get(f"login:{session_id}")
        if not raw:
            raise RuntimeError("Invalid or expired captcha session")
        info = json.loads(raw)
        data = info["hidden"].copy()
        data.update({
            "login_username": settings.rutracker_login,
            "login_password": settings.rutracker_password,
            "cap_sid":        info["hidden"]["cap_sid"],
            info["code_field"]: solution,
            "login":          info["submit_val"],
        })
        login_url = f"{self.base_url}/forum/login.php"
        r = self.scraper.post(login_url, data=data, headers={"Referer": login_url}, allow_redirects=False)
        loc = r.headers.get("Location")
        if loc and r.status_code in (302,303):
            self.scraper.get(urljoin(self.base_url+"/forum/", loc)).raise_for_status()
        else:
            r.raise_for_status()

        # «Прогрев» после CAPTCHA
        self.scraper.get(f"{self.base_url}/forum/tracker.php").raise_for_status()

        jar = self.scraper.cookies.get_dict()
        if not jar.get("bb_session"):
            raise RuntimeError("Login failed after captcha")
        self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)

    def _get_torrent_blob(self, tid: int) -> bytes:
        key = f"torrentblob:{tid}"
        if blob := self.redis.get(key):
            return blob
        data = self._download_sync(tid)
        self.redis.set(key, data, ex=self.blob_ttl)
        return data

    def _parse_filelist(self, bts: bytes) -> List[str]:
        meta = bencodepy.decode(bts)[b"info"]
        if b"filename" in meta:
            return [meta[b"filename"].decode(errors="ignore")]
        paths = []
        for f in meta.get(b"files", []):
            parts = [p.decode(errors="ignore") for p in f[b"path"]]
            paths.append("/".join(parts))
        return paths

    def _get_filelist(self, tid: int) -> List[str]:
        key = f"tracklist:{tid}"
        if raw := self.redis.get(key):
            return json.loads(raw)
        fl = self._parse_filelist(self._get_torrent_blob(tid))
        self.redis.set(key, json.dumps(fl), ex=self.filelist_ttl)
        return fl

    def _search_sync(self, query: str, only_lossless: Optional[bool], track: Optional[str]) -> List[TorrentInfo]:
        cache_key  = f"search:{query}:{only_lossless}:{track}"
        search_url = f"{self.base_url}/forum/tracker.php"

        # Убедимся, что залогинены
        self._ensure_login()

        # Обёртка GET с проверкой редиректа на login.php
        def guarded_get(params):
            r = self.scraper.get(search_url, params=params, allow_redirects=False)
            if r.status_code in (301, 302) and 'login.php' in (r.headers.get('Location') or ''):
                log.debug("Session expired on search GET, re-login")
                self.scraper.cookies.clear()
                self.redis.delete("rutracker:cookiejar")
                self._login_sync()
                r = self.scraper.get(search_url, params=params, allow_redirects=False)
            return r

        # 1) GET первой страницы
        r0 = guarded_get({"nm": query})
        r0.raise_for_status()
        html0 = r0.text or ""
        if 'id="login-form-full"' in html0:
            log.debug("Search GET returned login page, re-login")
            self.scraper.cookies.clear()
            self.redis.delete("rutracker:cookiejar")
            self._login_sync()
            r0 = guarded_get({"nm": query})
            r0.raise_for_status()
        self._last_html = r0.text

        # 2) POST первой страницы
        token = self._extract_form_token(r0.text)
        post_data = {"nm": query, "f[]": "-1"}
        if token:
            post_data["form_token"] = token

        r1 = self.scraper.post(search_url, data=post_data, allow_redirects=False)
        if r1.status_code in (301, 302) and 'login.php' in (r1.headers.get('Location') or ''):
            log.debug("Session expired on search POST, re-login")
            self.scraper.cookies.clear()
            self.redis.delete("rutracker:cookiejar")
            self._login_sync()
            r1 = self.scraper.post(search_url, data=post_data, allow_redirects=False)

        r1.raise_for_status()
        html1 = r1.text or ""
        if 'id="login-form-full"' in html1:
            log.debug("Search POST returned login page, re-login")
            self.scraper.cookies.clear()
            self.redis.delete("rutracker:cookiejar")
            self._login_sync()
            r1 = self.scraper.post(search_url, data=post_data, allow_redirects=False)
            r1.raise_for_status()

        doc = lxml.html.fromstring(r1.text)

        # 3) Парсинг
        parsed: List[Tuple[TorrentInfo,int]] = []
        for row in doc.xpath("//table[@id='tor-tbl']//tr[@data-topic_id]"):
            hrefs = row.xpath(".//a[contains(@href,'dl.php?t=')]/@href")
            if not hrefs:
                continue
            tid       = int(row.get("data-topic_id"))
            forum_txt = (row.xpath(".//td[contains(@class,'f-name-col')]//a/text()") or [""])[0].strip()
            title_txt = (row.xpath(".//td[contains(@class,'t-title-col')]//a/text()") or [""])[0].strip()
            combined  = f"{forum_txt} {title_txt}".strip()

            is_l = bool(_LOSSLESS_RE.search(combined))
            is_y = bool(_LOSSY_RE.search(combined))
            if only_lossless is True  and (not is_l or is_y): continue
            if only_lossless is False and is_l: continue

            size   = (row.xpath(".//td[contains(@class,'tor-size')]//a/text()") or [""])[0].strip()
            se     = int((row.xpath(".//b[contains(@class,'seedmed')]/text()") or ["0"])[0].strip())
            le     = int((row.xpath(".//td[contains(@class,'leechmed')]/text()") or ["0"])[0].strip())
            url_dl = urljoin(self.base_url + "/forum/", hrefs[0])

            parsed.append((TorrentInfo(
                title=combined, url=url_dl, size=size, seeders=se, leechers=le
            ), tid))

        # 4) Фильтрация по треку
        if track:
            from rapidfuzz import fuzz
            results = []
            t_low = track.lower()
            for ti, tid in parsed:
                if t_low in ti.title.lower():
                    results.append(ti)
                    continue
                try:
                    files = self._get_filelist(tid)
                except CaptchaRequired:
                    continue
                if any(t_low in f.lower() or fuzz.partial_ratio(t_low, f.lower()) >= 80 for f in files):
                    results.append(ti)
            final = results
        else:
            final = [ti for ti, _ in parsed]

        # 5) Кеширование и возврат
        to_cache = [
            {"title": r.title, "url": r.url, "size": r.size,
             "seeders": r.seeders, "leechers": r.leechers}
            for r in final
        ]
        self.redis.set(cache_key, json.dumps(to_cache), ex=self.search_ttl)
        return final

    async def search(self, query, only_lossless=None, track=None):
        import asyncio
        return await asyncio.to_thread(self._search_sync, query, only_lossless, track)

    def _download_sync(self, topic_id: int) -> bytes:
        self._ensure_login()
        dl_url = f"{self.base_url}/forum/dl.php?t={topic_id}"
        resp = self.scraper.get(dl_url, allow_redirects=True)
        resp.raise_for_status()
        ctype = resp.headers.get("Content-Type","")
        if "application/x-bittorrent" in ctype:
            return resp.content

        # Если вернулся HTML вместо торрента
        html = resp.text; self._last_html = html
        os.makedirs(self.dump_dir, exist_ok=True)
        path = os.path.join(self.dump_dir, f"download_error_{topic_id}_{int(time.time())}.html")
        with open(path, "w", encoding="utf-8") as f: f.write(html)
        log.error("Download failed, dumped HTML to %s", path)

        if "исчерпали суточный лимит" in html:
            raise HTTPException(status_code=429, detail="Daily download limit exceeded (1000/day)")

        doc = lxml.html.fromstring(html)
        hidden      = {inp.get("name"): inp.get("value","") for inp in doc.xpath(".//input[@type='hidden']") if inp.get("name")}
        code_fields = doc.xpath(".//input[starts-with(@name,'cap_code_')]/@name")
        imgs        = doc.xpath("//img[contains(@src,'/captcha/')]/@src")
        if not code_fields or not imgs:
            raise RuntimeError(f"Download parsing failed — HTML dumped to {path}")

        sid        = hidden.get("cap_sid", uuid.uuid4().hex)
        img_url    = urljoin(self.base_url+"/forum/", imgs[0])
        code_field = code_fields[0]
        submit_val = doc.xpath(".//input[@type='submit']/@value")[0]
        self.redis.set(f"login:{sid}", json.dumps({
            "hidden":     hidden,
            "code_field": code_field,
            "submit_val": submit_val,
        }), ex=300)

        raise CaptchaRequired(sid, img_url)

    async def download(self, topic_id: int):
        import asyncio
        return await asyncio.to_thread(self._download_sync, topic_id)

    async def close(self):
        pass