import logging

# ———————————————————————————————————————————————————————————————————————————————————————————————————————————————
# Настраиваем корневой логгер, чтобы он писал DEBUG и выводил имя логгера
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
# ———————————————————————————————————————————————————————————————————————————————————————————————————————————————

from fastapi import FastAPI
from spotiflac_backend.api.v1.health import router as health_router
from spotiflac_backend.api.v1.torrents import router as torrents_router

app = FastAPI(title="SpotiFlac Backend")

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