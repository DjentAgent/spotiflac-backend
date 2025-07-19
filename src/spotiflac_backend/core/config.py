# src/spotiflac_backend/core/config.py

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # где брать .env
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    # prod‑зависимые настройки
    redis_url: str = "redis://localhost:6379/0"
    rutracker_base: str = "https://rutracker.org"
    torrent_path: str = "/tmp/torrents"

    # теперь добавляем поля для логина
    rutracker_login: str        # будет браться из RUTRACKER_LOGIN
    rutracker_password: str     # будет браться из RUTRACKER_PASSWORD

# единственный экземпляр конфига
settings = Settings()


