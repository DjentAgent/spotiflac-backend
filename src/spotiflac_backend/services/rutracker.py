import json
import logging
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from math import ceil
from typing import List, Optional, Tuple
from urllib.parse import urljoin, parse_qs, urlparse

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
        self.cookie_ttl = getattr(settings, "rutracker_cookie_ttl", 24*3600)
        log.debug("Init with base_url=%s", self.base_url)
        # restore cookies
        try:
            raw = self.redis.get("rutracker:cookiejar")
            if raw:
                self.scraper.cookies.update(json.loads(raw))
        except:
            log.exception("Cookie restore failed")

    def _extract_form_token(self, html: str) -> str:
        m = _FORM_TOKEN_RE.search(html)
        return m.group(1) if m else ""

    def initiate_login(self) -> Tuple[Optional[str], Optional[str]]:
        login_url = f"{self.base_url}/forum/login.php"
        log.debug("→ [initiate_login] GET %s", login_url)
        resp = self.scraper.get(login_url);
        resp.raise_for_status()
        doc = lxml.html.fromstring(resp.text)

        # ищем форму и скрытые поля
        form = doc.get_element_by_id("login-form-full")
        hidden = {
            inp.get("name"): inp.get("value", "")
            for inp in form.xpath(".//input[@type='hidden']")
            if inp.get("name")
        }
        log.debug("→ [initiate_login] Hidden fields: %s", hidden)

        # если нет cap_sid — то капчи нет
        sid = hidden.get("cap_sid")
        captcha_imgs = doc.xpath("//img[contains(@src,'/captcha/')]/@src")
        if not captcha_imgs or not sid:
            log.debug("→ [initiate_login] No CAPTCHA required")
            return None, None

        code_fields = form.xpath(".//input[starts-with(@name,'cap_code_')]/@name")
        if not code_fields:
            log.error("→ [initiate_login] CAPTCHA_PARSE_ERROR: no code field")
            raise RuntimeError("CAPTCHA_PARSE_ERROR")
        code_field = code_fields[0]
        submit_val = form.xpath(".//input[@type='submit']/@value")[0]
        log.debug("→ [initiate_login] cap_sid=%s, code_field=%s, submit_val=%s", sid, code_field, submit_val)

        # сохраняем в Redis весь контекст для завершения
        session_id = uuid.uuid4().hex
        self.redis.set(
            f"login:{session_id}",
            json.dumps({
                "hidden": hidden,
                "code_field": code_field,
                "submit_val": submit_val,
            }),
            ex=300
        )

        # публичная ссылка на капчу
        img_src = captcha_imgs[0]
        img_url = urljoin(self.base_url + "/forum/", img_src)
        log.debug("→ [initiate_login] Raising CaptchaRequired(session_id=%s, img_url=%s)", session_id, img_url)
        raise CaptchaRequired(session_id, img_url)

    def complete_login(self, session_id: str, solution: str):
        """
        Завершает вход капчей и гарантирует, что мы действительно залогинены.
        """
        log.debug("→ [complete_login] session_id=%s, solution=%s", session_id, solution)
        raw = self.redis.get(f"login:{session_id}")
        if not raw:
            log.error("→ [complete_login] No Redis entry for session %s", session_id)
            raise RuntimeError("Invalid or expired captcha session")
        info = json.loads(raw)

        hidden = info["hidden"]
        sid = hidden.get("cap_sid")
        code_field = info["code_field"]
        submit_val = info["submit_val"]

        # 1) Собираем POST‑дату
        data = hidden.copy()
        data.update({
            "login_username": settings.rutracker_login,
            "login_password": settings.rutracker_password,
            "cap_sid": sid,
            code_field: solution,
            "login": submit_val,
        })
        login_url = f"{self.base_url}/forum/login.php"
        log.debug("→ [complete_login] POST %s", login_url)

        # 2) Отправляем и не следуем сразу за редиректом
        resp = self.scraper.post(
            login_url,
            data=data,
            headers={"Referer": login_url},
            allow_redirects=False
        )
        log.debug("→ [complete_login] POST status: %s", resp.status_code)

        # Если нас редиректят — берём URL из Location
        loc = resp.headers.get("Location")
        if loc and resp.status_code in (301, 302, 303):
            next_url = urljoin(self.base_url + "/forum/", loc)
            log.debug("→ [complete_login] Following redirect to %s", next_url)
            resp2 = self.scraper.get(next_url)
            resp2.raise_for_status()
        else:
            resp.raise_for_status()

        # 3) Обязательно прогоняем GET по трекеру, чтобы все куки точно установились
        tracker_url = f"{self.base_url}/forum/tracker.php"
        log.debug("→ [complete_login] Finalizing session with GET %s", tracker_url)
        final = self.scraper.get(tracker_url)
        final.raise_for_status()

        # 4) Проверяем наличие хотя бы bb_session
        jar = self.scraper.cookies.get_dict()
        log.debug("→ [complete_login] Cookies after finalize: %s", jar)
        if not jar.get("bb_session"):
            log.error("→ [complete_login] LOGIN_AFTER_CAPTCHA failed, cookies: %s", jar)
            raise RuntimeError("Login failed after captcha")

        # 5) Сохраняем в Redis
        self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)
        log.debug("→ [complete_login] CAPTCHA login succeeded; cookies saved")




    def _login_sync(self):
        url = f"{self.base_url}/forum/login.php"
        r = self.scraper.get(url); r.raise_for_status()
        token = self._extract_form_token(r.text)
        doc = lxml.html.fromstring(r.text)
        form = doc.get_element_by_id("login-form-full")
        data = {i.get("name"):i.get("value","") for i in form.xpath(".//input[@type='hidden']") if i.get("name")}
        if token:
            data["form_token"]=token
        sv = form.xpath(".//input[@type='submit']/@value")[0]
        data.update({
            "login_username": settings.rutracker_login,
            "login_password": settings.rutracker_password,
            "login": sv,
        })
        post = self.scraper.post(url, data=data, headers={"Referer":url}); post.raise_for_status()
        jar = self.scraper.cookies.get_dict()
        if not (jar.get("bb_session") or jar.get("bb_sessionhash")):
            raise RuntimeError("LOGIN_NO_SESSION")
        self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)

    def _ensure_login(self):
        """
        Гарантирует, что есть живая сессия. Если нет — сначала обычный логин,
        а при отсутствии куки bb_session — инициирует CAPTCHA.
        """
        jar = self.scraper.cookies.get_dict()
        if jar.get("bb_session"):
            return

        try:
            # пробуем без капчи
            self._login_sync()
        except RuntimeError as e:
            if str(e) == "LOGIN_NO_SESSION":
                # нет сессии — жёстко падаем в CAPTCHA
                self.initiate_login()
            else:
                raise

    def _parse_filelist(self,bts:bytes)->List[str]:
        meta = bencodepy.decode(bts)[b"info"]
        if b"filename" in meta:
            return [meta[b"filename"].decode(errors="ignore")]
        paths=[]
        for f in meta.get(b"files",[]):
            parts=[p.decode(errors="ignore") for p in f[b"path"]]
            paths.append("/".join(parts))
        return paths

    def _contains_track(self,fl,track):
        t=track.lower()
        return any(t in f.lower() or fuzz.partial_ratio(t,f.lower())>=80 for f in fl)

    def _search_sync(self, query: str, only_lossless: Optional[bool], track: Optional[str]) -> List[TorrentInfo]:
        cache_key = f"search:{query}:{only_lossless}:{track}"
        if raw := self.redis.get(cache_key):
            return [TorrentInfo(**d) for d in json.loads(raw)]

        # 1) Убедиться в логине/инициировать капчу
        self._ensure_login()

        search_url = f"{self.base_url}/forum/tracker.php"
        # 2) GET чтобы получить form_token
        r0 = self.scraper.get(search_url, params={"nm": query})
        r0.raise_for_status()
        token = self._extract_form_token(r0.text)

        # 3) POST запрос
        post_data = {"nm": query, "f[]": "-1"}
        if token:
            post_data["form_token"] = token
        r1 = self.scraper.post(search_url, data=post_data)
        r1.raise_for_status()
        doc1 = lxml.html.fromstring(r1.text)

        # 4) Считаем страницы
        total = int(_TOTAL_RE.search(doc1.text_content()).group(1)) if _TOTAL_RE.search(doc1.text_content()) else 0
        per_page = 50
        pages = ceil(total / per_page) if total else 1

        # 5) Ищем search_id
        search_id = None
        for script in doc1.xpath("//script/text()"):
            if m := _PG_BASE_URL_RE.search(script):
                search_id = parse_qs(m.group(1)).get("search_id", [None])[0]
                break

        parsed: List[Tuple[TorrentInfo, int]] = []

        # 6) Разбор одной страницы
        def _parse_page(doc):
            for row in doc.xpath("//table[@id='tor-tbl']//tr[@data-topic_id]"):
                tid = int(row.get("data-topic_id"))

                # forum + title
                forum = row.xpath(".//td[contains(@class,'f-name-col')]//a/text()")
                title = row.xpath(".//td[contains(@class,'t-title-col')]//a/text()")
                if not title:
                    continue
                forum_txt = forum[0].strip() if forum else ""
                title_txt = title[0].strip()
                combined = f"{forum_txt} {title_txt}".strip()

                # lossless/lossy
                is_l = bool(_LOSSLESS_RE.search(combined))
                is_y = bool(_LOSSY_RE.search(combined))
                if only_lossless is True and (not is_l or is_y):
                    continue
                if only_lossless is False and is_l:
                    continue

                # size
                size_el = row.xpath(".//td[contains(@class,'tor-size')]//a/text()")
                size = size_el[0].strip() if size_el else ""

                # seeders
                st = row.xpath(".//b[contains(@class,'seedmed')]/text()")
                st0 = st[0].strip() if st else "0"
                se = int(st0) if st0.isdigit() else 0
                # leechers
                lt = row.xpath(".//td[contains(@class,'leechmed')]/text()")
                lt0 = lt[0].strip() if lt else "0"
                le = int(lt0) if lt0.isdigit() else 0

                parsed.append((
                    TorrentInfo(
                        # сохраняем forum+title в title
                        title=combined,
                        url=urljoin(self.base_url + "/forum/", row.xpath(".//a[contains(@href,'dl.php?t=')]/@href")[0]),
                        size=size,
                        seeders=se,
                        leechers=le
                    ),
                    tid
                ))

        # 7) Первый проход
        _parse_page(doc1)

        # 8) Остальные страницы
        if pages > 1 and search_id:
            offsets = [i * per_page for i in range(1, pages)]
            def fetch(off):
                resp = self.scraper.get(search_url, params={"search_id": search_id, "start": off})
                resp.raise_for_status()
                _parse_page(lxml.html.fromstring(resp.text))

            with ThreadPoolExecutor(max_workers=min(4, len(offsets))) as ex:
                ex.map(fetch, offsets)

        # 9) Фильтрация по треку (в title уже есть forum+title)
        if track:
            t = track.lower()
            results = [ti for ti, _ in parsed if t in ti.title.lower()]
        else:
            results = [ti for ti, _ in parsed]

        # 10) Кэшируем и возвращаем
        to_cache = [{
            "title": r.title, "url": r.url,
            "size": r.size, "seeders": r.seeders, "leechers": r.leechers
        } for r in results]
        self.redis.set(cache_key, json.dumps(to_cache), ex=300)
        return results





    async def search(self,query,only_lossless=None,track=None):
        import asyncio
        return await asyncio.to_thread(self._search_sync,query,only_lossless,track)

    def _download_sync(self, topic_id: int) -> bytes:
        """
        Скачиваем .torrent, даем cloudflare‑редиректу пройти (allow_redirects=True),
        и только если контент не .torrent — считаем, что нужна капча.
        """
        self._ensure_login()
        dl_url = f"{self.base_url}/forum/dl.php?t={topic_id}"
        log.debug("→ [download] GET %s (with redirects)", dl_url)
        resp = self.scraper.get(dl_url, allow_redirects=True)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        # реальный .torrent обычно application/x-bittorrent
        if "text/html" in content_type:
            log.warning("→ [download] HTML received instead of .torrent, need captcha for topic %s", topic_id)
            raise CaptchaRequired(session_id=uuid.uuid4().hex,
                                  img_url=f"{self.base_url}/forum/login.php")
        return resp.content

    async def download(self,topic_id:int):
        import asyncio
        return await asyncio.to_thread(self._download_sync,topic_id)

    async def close(self):
        pass
