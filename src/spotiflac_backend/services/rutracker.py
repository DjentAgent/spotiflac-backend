import json
import logging
import re
import uuid
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
        log.debug("→ [complete_login] sid=%s, code_field=%s, submit_val=%s", sid, code_field, submit_val)

        # готовим данные для POST
        data = hidden.copy()
        data.update({
            "login_username": settings.rutracker_login,
            "login_password": settings.rutracker_password,
            "cap_sid": sid,
            code_field: solution,
            "login": submit_val,
        })
        log.debug("→ [complete_login] POST data keys: %s", list(data.keys()))

        login_url = f"{self.base_url}/forum/login.php"
        log.debug("→ [complete_login] POST %s", login_url)
        post = self.scraper.post(login_url, data=data, headers={"Referer": login_url})
        log.debug("→ [complete_login] Response status: %s", post.status_code)
        snippet = post.text[:200].replace("\n", " ")
        log.debug("→ [complete_login] Response text snippet: %s", snippet)

        post.raise_for_status()

        jar = self.scraper.cookies.get_dict()
        log.debug("→ [complete_login] Cookies after POST: %s", jar)
        if not (jar.get("bb_session") or jar.get("bb_sessionhash")):
            log.error("→ [complete_login] LOGIN_AFTER_CAPTCHA failed, cookies: %s", jar)
            raise RuntimeError("Login failed after captcha")

        # сохраняем новые куки
        self.redis.set("rutracker:cookiejar", json.dumps(jar), ex=self.cookie_ttl)
        log.debug("→ [complete_login] CAPTCHA login succeeded; session cookie saved")


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
        jar = self.scraper.cookies.get_dict()
        if jar.get("bb_session") or jar.get("bb_sessionhash"):
            return
        try:
            self._login_sync()
        except RuntimeError as e:
            if str(e)=="LOGIN_NO_SESSION":
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

        # Убедиться, что залогинились или получить капчу
        self._ensure_login()

        # Шаг 1: получить форму поиска
        search_url = f"{self.base_url}/forum/tracker.php"
        r0 = self.scraper.get(search_url, params={"nm": query});
        r0.raise_for_status()
        doc0 = lxml.html.fromstring(r0.text)
        forms = doc0.xpath("//form[@id='tr-form']")
        if not forms:
            raise RuntimeError("Search form not found; login might have failed")
        form = forms[0]

        # Шаг 2: отправить запрос поиска
        action = form.get("action") or search_url
        if not action.startswith("http"):
            action = f"{self.base_url}/forum/{action.lstrip('/')}"
        post_data = {
            inp.get("name"): inp.get("value", "")
            for inp in form.xpath(".//input[@type='hidden']")
            if inp.get("name")
        }
        post_data.update({"nm": query, "f[]": "-1"})
        r1 = self.scraper.post(action, data=post_data);
        r1.raise_for_status()
        doc1 = lxml.html.fromstring(r1.text)

        # Шаг 3: подсчитать страницы
        total = int(_TOTAL_RE.search(doc1.text_content()).group(1)) if _TOTAL_RE.search(doc1.text_content()) else 0
        per_page = 50
        pages = ceil(total / per_page) if total else 1

        # Шаг 4: найти search_id для пагинации
        parsed: List[Tuple[TorrentInfo, int]] = []
        search_id = None
        for script in doc1.xpath("//script/text()"):
            if m := _PG_BASE_URL_RE.search(script):
                search_id = parse_qs(m.group(1)).get("search_id", [None])[0]
                break

        # Вспомогательная функция для парсинга одной страницы
        def parse_page(doc):
            for row in doc.xpath("//tr[@data-topic_id]"):
                hrefs = row.xpath(".//a[contains(@href,'dl.php?t=')]/@href")
                if not hrefs:
                    continue
                href = hrefs[0]
                tid = int(parse_qs(urlparse(href).query)["t"][0])

                titles = row.xpath(".//td[contains(@class,'t-title-col')]//a/text()")
                if not titles:
                    continue
                title = titles[0].strip()

                is_l = bool(_LOSSLESS_RE.search(title))
                is_y = bool(_LOSSY_RE.search(title))
                if only_lossless is True and (not is_l or is_y):
                    continue
                if only_lossless is False and is_l:
                    continue

                sizes = row.xpath(".//a[contains(@href,'dl.php?t=')]/text()")
                if not sizes:
                    continue
                size = sizes[0].strip()

                seeders_txt = row.xpath(".//b[contains(@class,'seedmed')]/text()")
                leechers_txt = row.xpath(".//td[contains(@class,'leechmed')]/text()")
                se = int(seeders_txt[0] or 0) if seeders_txt else 0
                le = int(leechers_txt[0] or 0) if leechers_txt else 0

                parsed.append((
                    TorrentInfo(
                        title=title,
                        url=urljoin(self.base_url + "/forum/", href),
                        size=size,
                        seeders=se,
                        leechers=le
                    ),
                    tid
                ))

        # Парсим первую страницу
        parse_page(doc1)
        # Парсим остальные, если есть
        if pages > 1 and search_id:
            for off in range(1, pages):
                resp = self.scraper.get(search_url, params={"search_id": search_id, "start": off * per_page})
                resp.raise_for_status()
                parse_page(lxml.html.fromstring(resp.text))

        # Фильтрация по имени трека
        if track:
            results: List[TorrentInfo] = []
            for ti, tid in parsed:
                cache2 = f"tracklist:{tid}"
                if raw2 := self.redis.get(cache2):
                    fl = json.loads(raw2)
                else:
                    # при загрузке могло потребоваться капча
                    try:
                        torrent_bytes = self._download_sync(tid)
                    except CaptchaRequired:
                        raise
                    except Exception as e:
                        log.warning("Skipping %s: download/parsing error %s", tid, e)
                        continue

                    try:
                        fl = self._parse_filelist(torrent_bytes)
                    except Exception as e:
                        log.warning("Skipping %s: bencode parse error %s", tid, e)
                        continue

                    self.redis.set(cache2, json.dumps(fl), ex=3600)

                if self._contains_track(fl, track):
                    results.append(ti)
        else:
            results = [ti for ti, _ in parsed]

        # Кэшируем и возвращаем
        out = [
            {"title": r.title, "url": r.url, "size": r.size, "seeders": r.seeders, "leechers": r.leechers}
            for r in results
        ]
        self.redis.set(cache_key, json.dumps(out), ex=300)
        return results

    async def search(self,query,only_lossless=None,track=None):
        import asyncio
        return await asyncio.to_thread(self._search_sync,query,only_lossless,track)

    def _download_sync(self,topic_id:int)->bytes:
        self._ensure_login()
        url=f"{self.base_url}/forum/dl.php?t={topic_id}"
        r=self.scraper.get(url,allow_redirects=True); r.raise_for_status()
        return r.content

    async def download(self,topic_id:int):
        import asyncio
        return await asyncio.to_thread(self._download_sync,topic_id)

    async def close(self):
        pass
