import json
import logging
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from math import ceil
from typing import List, Optional, Tuple
from urllib.parse import urljoin, parse_qs

import bencodepy
import cloudscraper
import lxml.html
import redis
from rapidfuzz import fuzz

from spotiflac_backend.core.config import settings
from spotiflac_backend.models.torrent import TorrentInfo

log = logging.getLogger(__name__)

# Регэкспы
_LOSSLESS_RE    = re.compile(r"\b(flac|wavpack|wv|ape|alac|aiff|pcm|dts|mlp|tta|mqa|lossless)\b", re.IGNORECASE)
_LOSSY_RE       = re.compile(r"\b(mp3|aac|ogg|opus|lossy)\b", re.IGNORECASE)
_PG_BASE_URL_RE = re.compile(r"PG_BASE_URL\s*:\s*['\"][^?]+\?([^'\"]+)['\"]", re.IGNORECASE)
_TOTAL_RE       = re.compile(r"Результатов поиска:\s*(\d+)", re.IGNORECASE)
_FORM_TOKEN_RE  = re.compile(r"form_token\s*:\s*'([0-9a-f]+)'")

class CaptchaRequired(Exception):
    def __init__(self, session_id: str, img_url: str):
        super().__init__(f"Captcha required: {session_id}")
        self.session_id = session_id
        self.img_url = img_url

class RutrackerService:
    def __init__(self, base_url: str = None):
        self.base_url = (base_url or settings.rutracker_base).rstrip("/")
        self.scraper = cloudscraper.create_scraper(
            browser={"browser":"chrome","platform":"windows","mobile":False},
            delay=10
        )
        self.redis = redis.Redis.from_url(settings.redis_url)
        self.cookie_ttl   = getattr(settings, "rutracker_cookie_ttl",   24*3600)
        self.filelist_ttl = getattr(settings, "rutracker_filelist_ttl", 24*3600)
        self.blob_ttl     = getattr(settings, "rutracker_blob_ttl",     7*24*3600)
        self.search_ttl   = getattr(settings, "rutracker_search_ttl",   5*60)
        log.debug("Init with base_url=%s", self.base_url)
        # восстановить куки из Redis
        try:
            raw = self.redis.get("rutracker:cookiejar")
            if raw:
                self.scraper.cookies.update(json.loads(raw))
        except Exception:
            log.exception("Cookie restore failed")

    # --------------- LOGIN / CAPTCHA ---------------
    def _extract_form_token(self, html: str) -> str:
        m = _FORM_TOKEN_RE.search(html)
        return m.group(1) if m else ""

    def _login_sync(self):
        """
        GET-login.php -> POST-login.php -> сохраняем bb_session
        (без login_try, именно так, как в последней рабочей версии).
        """
        url = f"{self.base_url}/forum/login.php"
        # 1) GET формы
        resp = self.scraper.get(url)
        resp.raise_for_status()
        token = self._extract_form_token(resp.text)
        doc = lxml.html.fromstring(resp.text)
        form = doc.get_element_by_id("login-form-full")

        # 2) Собираем скрытые поля
        data = {
            inp.get("name"): inp.get("value", "")
            for inp in form.xpath(".//input[@type='hidden']")
            if inp.get("name")
        }
        if token:
            data["form_token"] = token

        # 3) Подставляем креды
        submit_val = form.xpath(".//input[@type='submit']/@value")[0]
        data.update({
            "login_username": settings.rutracker_login,
            "login_password": settings.rutracker_password,
            "login": submit_val,
        })

        # 4) POST и следим за редиректом автоматически
        post = self.scraper.post(url, data=data, headers={"Referer": url}, allow_redirects=True)
        post.raise_for_status()

        # 5) Проверяем куки
        jar = self.scraper.cookies.get_dict()
        if not jar.get("bb_session"):
            # сюда можно вставить CaptchaRequired, но пока — просто падение
            raise RuntimeError("LOGIN_NO_SESSION")

        # 6) Сохраняем в Redis
        self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)
        log.debug("→ [login] Success, bb_session=%s", jar.get("bb_session"))

    def _ensure_login(self):
        """
        логинимся только если нет bb_session в текущих куках
        """
        jar = self.scraper.cookies.get_dict()
        if jar.get("bb_session"):
            return
        log.debug("→ [ensure_login] no bb_session, logging in")
        self._login_sync()

    def initiate_login(self) -> Tuple[Optional[str], Optional[str]]:
        # ... (оставляем без изменений: генерим CaptchaRequired)
        login_url = f"{self.base_url}/forum/login.php"
        resp = self.scraper.get(login_url); resp.raise_for_status()
        doc = lxml.html.fromstring(resp.text)
        form = doc.get_element_by_id("login-form-full")
        hidden = {
            inp.get("name"): inp.get("value","")
            for inp in form.xpath(".//input[@type='hidden']")
            if inp.get("name")
        }
        sid = hidden.get("cap_sid")
        imgs = doc.xpath("//img[contains(@src,'/captcha/')]/@src")
        if not sid or not imgs:
            return None, None
        code_field = form.xpath(".//input[starts-with(@name,'cap_code_')]/@name")[0]
        submit_val = form.xpath(".//input[@type='submit']/@value")[0]
        session_id = uuid.uuid4().hex
        self.redis.set(f"login:{session_id}", json.dumps({
            "hidden": hidden,
            "code_field": code_field,
            "submit_val": submit_val,
        }), ex=300)
        img_url = urljoin(self.base_url + "/forum/", imgs[0])
        raise CaptchaRequired(session_id, img_url)

    def complete_login(self, session_id: str, solution: str):
        # ... (оставляем без изменений)
        raw = self.redis.get(f"login:{session_id}")
        if not raw:
            raise RuntimeError("Invalid or expired captcha session")
        info = json.loads(raw)
        hidden = info["hidden"]
        data = hidden.copy()
        data.update({
            "login_username": settings.rutracker_login,
            "login_password": settings.rutracker_password,
            "cap_sid": hidden["cap_sid"],
            info["code_field"]: solution,
            "login": info["submit_val"],
        })
        login_url = f"{self.base_url}/forum/login.php"
        resp = self.scraper.post(login_url, data=data, headers={"Referer":login_url}, allow_redirects=False)
        loc = resp.headers.get("Location")
        if loc and resp.status_code in (302,303):
            nxt = urljoin(self.base_url + "/forum/", loc)
            self.scraper.get(nxt).raise_for_status()
        else:
            resp.raise_for_status()
        self.scraper.get(f"{self.base_url}/forum/tracker.php").raise_for_status()
        jar = self.scraper.cookies.get_dict()
        if not jar.get("bb_session"):
            raise RuntimeError("Login failed after captcha")
        self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)

    # --------------- TORNADO-BLOB CACHE ---------------
    def _get_torrent_blob(self, tid: int) -> bytes:
        key = f"torrentblob:{tid}"
        if blob := self.redis.get(key):
            return blob
        data = self._download_sync(tid)
        self.redis.set(key, data, ex=self.blob_ttl)
        return data

    def _get_filelist(self, tid: int) -> List[str]:
        key = f"tracklist:{tid}"
        if raw := self.redis.get(key):
            return json.loads(raw)
        fl = self._parse_filelist(self._get_torrent_blob(tid))
        self.redis.set(key, json.dumps(fl), ex=self.filelist_ttl)
        return fl

    # --------------- PARSING & SEARCH ---------------
    def _parse_filelist(self, bts: bytes) -> List[str]:
        meta = bencodepy.decode(bts)[b"info"]
        if b"filename" in meta:
            return [meta[b"filename"].decode(errors="ignore")]
        paths = []
        for f in meta.get(b"files", []):
            parts = [p.decode(errors="ignore") for p in f[b"path"]]
            paths.append("/".join(parts))
        return paths

    def _contains_track(self, fl: List[str], track: str) -> bool:
        t = track.lower()
        return any(t in fname.lower() or fuzz.partial_ratio(t, fname.lower()) >= 80 for fname in fl)

    def _search_sync(self, query: str, only_lossless: Optional[bool], track: Optional[str]) -> List[TorrentInfo]:
        cache_key = f"search:{query}:{only_lossless}:{track}"

        # 1) Убедиться, что сессия активна
        self._ensure_login()

        # 2) Кеш
        if raw := self.redis.get(cache_key):
            return [TorrentInfo(**d) for d in json.loads(raw)]

        search_url = f"{self.base_url}/forum/tracker.php"

        # 3) GET первой страницы без редиректов
        r0 = self.scraper.get(search_url, params={"nm": query}, allow_redirects=False)
        if r0.status_code in (301, 302) and "login.php" in (r0.headers.get("Location") or ""):
            log.debug("→ [search] session expired, re-login")
            self.scraper.cookies.clear()
            self.redis.delete("rutracker:cookiejar")
            self._ensure_login()
            r0 = self.scraper.get(search_url, params={"nm": query})
        r0.raise_for_status()

        token = self._extract_form_token(r0.text)
        post_data = {"nm": query, "f[]": "-1"}
        if token:
            post_data["form_token"] = token

        # 5) POST первой страницы
        r1 = self.scraper.post(search_url, data=post_data)
        r1.raise_for_status()
        doc1 = lxml.html.fromstring(r1.text)

        # 6) Пагинация
        text = doc1.text_content()
        total = int(_TOTAL_RE.search(text).group(1)) if _TOTAL_RE.search(text) else 0
        per_page = 50
        pages = ceil(total / per_page) if total else 1

        # 7) Получаем search_id
        search_id = None
        for script in doc1.xpath("//script/text()"):
            if m := _PG_BASE_URL_RE.search(script):
                search_id = parse_qs(m.group(1)).get("search_id", [None])[0]
                break

        parsed: List[Tuple[TorrentInfo,int]] = []
        def _parse_page(doc):
            for row in doc.xpath("//table[@id='tor-tbl']//tr[@data-topic_id]"):
                tid = int(row.get("data-topic_id"))
                forum_txt = (row.xpath(".//td[contains(@class,'f-name-col')]//a/text()") or [""])[0].strip()
                title_txt = (row.xpath(".//td[contains(@class,'t-title-col')]//a/text()") or [""])[0].strip()
                combined = f"{forum_txt} {title_txt}".strip()
                is_l = bool(_LOSSLESS_RE.search(combined))
                is_y = bool(_LOSSY_RE.search(combined))
                if only_lossless is True and (not is_l or is_y): continue
                if only_lossless is False and is_l: continue
                size = (row.xpath(".//td[contains(@class,'tor-size')]//a/text()") or [""])[0].strip()
                se = int((row.xpath(".//b[contains(@class,'seedmed')]/text()") or ["0"])[0].strip())
                le = int((row.xpath(".//td[contains(@class,'leechmed')]/text()") or ["0"])[0].strip())
                url = urljoin(self.base_url + "/forum/", row.xpath(".//a[contains(@href,'dl.php?t=')]/@href")[0])
                parsed.append((TorrentInfo(title=combined,url=url,size=size,seeders=se,leechers=le), tid))

        # 8) Первая страница
        _parse_page(doc1)

        # 9) Остальные страницы
        if pages>1 and search_id:
            offsets = [i*per_page for i in range(1,pages)]
            with ThreadPoolExecutor(max_workers=min(4,len(offsets))) as ex:
                for off in offsets:
                    resp = self.scraper.get(search_url, params={"search_id":search_id,"start":off}, allow_redirects=False)
                    if resp.status_code in (301,302) and "login.php" in (resp.headers.get("Location") or ""):
                        log.debug(f"→ [page {off}] session expired, re-login")
                        self.scraper.cookies.clear()
                        self.redis.delete("rutracker:cookiejar")
                        self._ensure_login()
                        resp = self.scraper.get(search_url, params={"search_id":search_id,"start":off})
                    resp.raise_for_status()
                    _parse_page(lxml.html.fromstring(resp.text))

        # 10) Фильтр по треку
        if track:
            results=[]
            t_low=track.lower()
            for ti,tid in parsed[:50]:
                if t_low in ti.title.lower():
                    results.append(ti); continue
                try:
                    files=self._get_filelist(tid)
                except CaptchaRequired:
                    continue
                if any(t_low in f.lower() or fuzz.partial_ratio(t_low,f.lower())>=80 for f in files):
                    results.append(ti)
            final=results
        else:
            final=[ti for ti,_ in parsed]

        # 11) Кешируем
        to_cache=[{"title":r.title,"url":r.url,"size":r.size,"seeders":r.seeders,"leechers":r.leechers}
                  for r in final]
        self.redis.set(cache_key, json.dumps(to_cache), ex=self.search_ttl)
        return final

    async def search(self, query, only_lossless=None, track=None):
        import asyncio
        return await asyncio.to_thread(self._search_sync, query, only_lossless, track)

    def _download_sync(self, topic_id: int) -> bytes:
        self._ensure_login()
        dl_url = f"{self.base_url}/forum/dl.php?t={topic_id}"
        log.debug("→ [download] GET %s", dl_url)
        resp = self.scraper.get(dl_url, allow_redirects=True)
        resp.raise_for_status()
        if "text/html" in (resp.headers.get("Content-Type","")):
            raise CaptchaRequired(session_id=uuid.uuid4().hex, img_url=f"{self.base_url}/forum/login.php")
        return resp.content

    async def download(self, topic_id: int):
        import asyncio
        return await asyncio.to_thread(self._download_sync, topic_id)

    async def close(self):
        pass
