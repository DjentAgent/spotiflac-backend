# syntax=docker/dockerfile:1

FROM python:3.10-slim

# 1) Системные зависимости
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2) Пинем Poetry на версию, где поддерживается --without=dev
RUN pip install --no-cache-dir "poetry>=1.2,<1.4"

# 3) Копируем только манифесты, чтобы закэшировать слой зависимостей
COPY pyproject.toml poetry.lock ./

# 4) Отключаем виртуальные окружения и ставим только main‑группу
RUN poetry config virtualenvs.create false \
 && poetry install --no-root --no-interaction --no-ansi --without=dev

# 5) Копируем весь код приложения
COPY . .

# 6) Чтобы импорт из src/spotiflac_backend работал
ENV PYTHONPATH=/app/src

EXPOSE 8000

# 7) Запуск
CMD ["uvicorn", "spotiflac_backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
