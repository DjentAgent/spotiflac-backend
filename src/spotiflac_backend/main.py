"""
SpotiFlac Backend - Optimized FastAPI Application
"""

import os
# Глобально отключаем hiredis, чтобы не видеть _AsyncHiredisParser баги
os.environ.setdefault("REDIS_DISABLE_HIREDIS", "1")

import logging
import sys
import time
import asyncio
import socket
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import uvloop
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from redis.asyncio import Redis, ConnectionPool
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from starlette.middleware.base import BaseHTTPMiddleware

from spotiflac_backend.core.config import settings
from spotiflac_backend.api.v1.health import router as health_router
from spotiflac_backend.api.v1.torrents import router as torrents_router
from spotiflac_backend.api.v1.spotify import router as spotify_router

# Импортируем сервисы для pre-warming
from spotiflac_backend.services.rutracker import get_rutracker_service
from spotiflac_backend.services.pirate_bay_service import get_piratebay_service

# --- Кросс-версийный импорт PythonParser (если доступен — используем)
try:
    from redis.connection import PythonParser as RedisPythonParser
except Exception:
    try:
        from redis._parsers import PythonParser as RedisPythonParser
    except Exception:
        RedisPythonParser: Optional[type] = None


# ========================= Logging Configuration =========================

class ColoredFormatter(logging.Formatter):
    grey = "\x1b[38;21m"
    blue = "\x1b[34m"
    green = "\x1b[32m"
    yellow = "\x1b[33m"
    red = "\x1b[31m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    COLORS = {
        logging.DEBUG: grey,
        logging.INFO: blue,
        logging.WARNING: yellow,
        logging.ERROR: red,
        logging.CRITICAL: bold_red
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelno, self.grey)
        record.levelname = f"{log_color}{record.levelname}{self.reset}"
        return super().format(record)


def setup_logging():
    log_level = getattr(settings, "log_level", "INFO").upper()
    numeric_level = getattr(logging, log_level, logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[]
    )

    console_handler = logging.StreamHandler(sys.stdout)
    if sys.stdout.isatty():
        console_handler.setFormatter(ColoredFormatter(
            "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
    else:
        console_handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)

    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    logging.getLogger("fastapi").setLevel(logging.INFO)
    logging.getLogger("redis").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logging.getLogger("spotiflac_backend").setLevel(logging.DEBUG)


# ========================= Middleware =========================

class TimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.perf_counter()
        request_id = request.headers.get("X-Request-ID", str(time.time()))
        request.state.request_id = request_id
        try:
            response = await call_next(request)
            process_time = time.perf_counter() - start_time
            response.headers["X-Process-Time"] = f"{process_time:.3f}"
            response.headers["X-Request-ID"] = request_id
            if process_time > 1.0:
                logging.warning(
                    f"Slow request: {request.method} {request.url.path} took {process_time:.3f}s"
                )
            return response
        except Exception as e:
            process_time = time.perf_counter() - start_time
            logging.error(
                f"Request failed: {request.method} {request.url.path} after {process_time:.3f}s: {e}"
            )
            raise


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, requests_per_minute: int = 60):
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.requests: Dict[str, list[float]] = {}

    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host
        now = time.time()
        minute_ago = now - 60

        self.requests = {
            ip: [t for t in times if t > minute_ago]
            for ip, times in self.requests.items()
            if any(t > minute_ago for t in times)
        }

        ip_requests = self.requests.get(client_ip, [])
        recent_requests = [t for t in ip_requests if t > minute_ago]

        if len(recent_requests) >= self.requests_per_minute:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "Too many requests. Please try again later."},
                headers={"Retry-After": "60"}
            )

        recent_requests.append(now)
        self.requests[client_ip] = recent_requests
        return await call_next(request)


# ========================= Redis Manager =========================

class RedisManager:
    """Централизованное управление Redis соединениями."""
    _pool_bytes: ConnectionPool | None = None
    _pool_text: ConnectionPool | None = None
    _client: Redis | None = None
    _cache_client: Redis | None = None

    @classmethod
    async def initialize(cls):
        if cls._pool_bytes is None or cls._pool_text is None:
            base_kwargs: Dict[str, Any] = dict(
                max_connections=getattr(settings, "redis_max_connections", 100),
                socket_keepalive=True,
                health_check_interval=30,
                socket_timeout=5,
                retry_on_timeout=True,
            )
            # Если доступен PythonParser — используем его (без hiredis)
            if RedisPythonParser is not None:
                base_kwargs["parser_class"] = RedisPythonParser

            # Раздельные пулы, чтобы не смешивать кодеки
            cls._pool_bytes = ConnectionPool.from_url(settings.redis_url, **base_kwargs)
            cls._pool_text  = ConnectionPool.from_url(settings.redis_url, **base_kwargs)

            cls._client = Redis(
                connection_pool=cls._pool_bytes,
                encoding="utf-8",
                decode_responses=False
            )
            cls._cache_client = Redis(
                connection_pool=cls._pool_text,
                encoding="utf-8",
                decode_responses=True
            )

        return cls._client, cls._cache_client

    @classmethod
    async def close(cls):
        if cls._client:
            await cls._client.close()
        if cls._cache_client:
            await cls._cache_client.close()
        if cls._pool_bytes:
            await cls._pool_bytes.disconnect()
        if cls._pool_text:
            await cls._pool_text.disconnect()
        cls._client = None
        cls._cache_client = None
        cls._pool_bytes = None
        cls._pool_text = None


# ========================= Lifespan Manager =========================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("🚀 Starting SpotiFlac Backend...")
    try:
        logging.info("Initializing Redis connections...")
        redis_client, cache_client = await RedisManager.initialize()
        await redis_client.ping()
        logging.info("✅ Redis connection established")

        FastAPICache.init(
            RedisBackend(cache_client),
            prefix="spotiflac:cache:",
            expire=getattr(settings, "cache_ttl", 300),
        )
        logging.info("✅ FastAPI Cache initialized")

        logging.info("Pre-warming services...")
        rt_service = get_rutracker_service()
        pb_service = get_piratebay_service()
        logging.info("✅ Services pre-warmed")

        app.state.redis_client = redis_client
        app.state.cache_client = cache_client
        app.state.rt_service = rt_service
        app.state.pb_service = pb_service

        logging.info("✨ Application startup complete!")
        yield

    except Exception as e:
        logging.error(f"❌ Startup failed: {e}")
        raise

    finally:
        logging.info("🛑 Shutting down SpotiFlac Backend...")
        try:
            if hasattr(app.state, "rt_service"):
                await app.state.rt_service.close()
                logging.info("Closed RuTracker service")
            if hasattr(app.state, "pb_service"):
                await app.state.pb_service.close()
                logging.info("Closed PirateBay service")

            await RedisManager.close()
            logging.info("✅ Redis connections closed")
        except Exception as e:
            logging.error(f"Error during shutdown: {e}")
        logging.info("👋 Application shutdown complete!")


# ========================= Application Factory =========================

def create_application() -> FastAPI:
    setup_logging()

    app = FastAPI(
        title="SpotiFlac Backend",
        description="High-performance torrent search and download API",
        version="2.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
        default_response_class=JSONResponse,
        openapi_tags=[
            {"name": "health", "description": "Health check endpoints"},
            {"name": "torrents", "description": "Torrent search and download"},
            {"name": "spotify", "description": "Spotify integration"},
        ],
    )

    # CORS: нативный клиент, без credentials
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Process-Time", "X-Request-ID"],
        max_age=86400,
    )

    # Security: проверка Host
    if hasattr(settings, "allowed_hosts"):
        app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=settings.allowed_hosts
        )

    # Compression
    app.add_middleware(
        GZipMiddleware,
        minimum_size=1000,
        compresslevel=6,
    )

    # Rate limiting
    if getattr(settings, "rate_limit_enabled", True):
        app.add_middleware(
            RateLimitMiddleware,
            requests_per_minute=getattr(settings, "rate_limit_rpm", 60),
        )

    # Timing (последний для измерения полного времени)
    app.add_middleware(TimingMiddleware)

    # Routers
    app.include_router(health_router, prefix="/health", tags=["health"])
    app.include_router(torrents_router, prefix="/api/v1/torrents", tags=["torrents"])
    app.include_router(spotify_router, prefix="/api/v1/spotify", tags=["spotify"])

    # Exception Handlers
    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": str(exc)}
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        logging.error(f"Unhandled exception: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"}
        )

    # Root
    @app.get("/", include_in_schema=False)
    async def root():
        return {
            "name": "SpotiFlac Backend",
            "version": "2.0.0",
            "status": "running",
            "docs": "/api/docs"
        }

    return app


# ========================= Main =========================

if sys.platform != "win32":
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

app = create_application()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=getattr(settings, "host", "0.0.0.0"),
        port=getattr(settings, "port", 8000),
        reload=getattr(settings, "debug", False),
        log_config=None,
        access_log=False,
        workers=1 if getattr(settings, "debug", False) else None,
        loop="uvloop" if sys.platform != "win32" else "asyncio",
    )