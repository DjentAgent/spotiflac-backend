import pytest
from httpx import AsyncClient
from spotiflac_backend.main import app

@pytest.mark.asyncio
async def test_health():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        resp = await ac.get("/api/v1/health/")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}