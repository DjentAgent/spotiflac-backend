FROM python:3.10-slim

# 1) System dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       curl \
       chromium \
       chromium-driver \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2) Install Poetry
RUN pip install --no-cache-dir "poetry>=1.2,<1.4"

# 3) Copy manifest files for caching dependencies
COPY pyproject.toml poetry.lock ./

# 4) Configure Poetry and install only main dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-root --no-interaction --no-ansi --without=dev

# 5) Copy application code
COPY . .

# 6) Set PYTHONPATH and browser paths for Selenium
ENV PYTHONPATH=/app/src \
    CHROME_BIN=/usr/bin/chromium \
    CHROME_DRIVER=/usr/bin/chromedriver

EXPOSE 8000

# 7) Run application
CMD ["uvicorn", "spotiflac_backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]