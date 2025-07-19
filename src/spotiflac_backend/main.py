import logging

# ———————————————————————————————————————————————————————————————————————————————————————————————————————————————
# Настраиваем корневой логгер, чтобы он писал DEBUG и выводил имя логгера
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
# ———————————————————————————————————————————————————————————————————————————————————————————————————————————————

from fastapi import FastAPI
import aioredis
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend

from spotiflac_backend.core.config import settings
from spotiflac_backend.api.v1.health import router as health_router
from spotiflac_backend.api.v1.torrents import router as torrents_router

app = FastAPI(title="SpotiFlac Backend")


@app.on_event("startup")
async def startup():
    """
    При старте приложения создаём подключение к Redis
    и инициализируем общий кеш для эндпоинтов.
    """
    # Подключаемся по URL из настроек, например redis://localhost:6379/0
    redis = await aioredis.from_url(
        settings.redis_url,
        encoding="utf8",
        decode_responses=True,
    )
    # Инициализируем бэкенд кеша с префиксом ключей "spotiflac"
    FastAPICache.init(RedisBackend(redis), prefix="spotiflac")


# health check
app.include_router(
    health_router,
    prefix="/api/v1/health",
    tags=["health"],
)

# torrents
app.include_router(
    torrents_router,
    prefix="/api/v1/torrents",
    tags=["torrents"],
)
