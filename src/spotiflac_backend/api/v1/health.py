from fastapi import APIRouter

# создаём роутер для health‑check
router = APIRouter()

@router.get("/", summary="Health check")
async def health_check():
    """
    Простейший endpoint для проверки, что сервис жив и отвечает.
    Возвращает {"status": "ok"}.
    """
    return {"status": "ok"}
