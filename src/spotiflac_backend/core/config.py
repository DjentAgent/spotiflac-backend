# src/spotiflac_backend/core/config.py

from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    rutracker_login: str = Field(..., env="RUTRACKER_LOGIN")
    rutracker_password: str = Field(..., env="RUTRACKER_PASSWORD")
    rutracker_base: str = Field("https://rutracker.org", env="RUTRACKER_BASE")
    redis_url: str = Field("redis://localhost:6379/0", env="REDIS_URL")
    rutracker_cookie_ttl: int = Field(86400, env="RUTRACKER_COOKIE_TTL")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "forbid"

settings = Settings()
