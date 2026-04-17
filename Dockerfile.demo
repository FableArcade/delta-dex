# Delta Dex — HuggingFace Spaces Docker deploy.
#
# Self-contained image: FastAPI + static frontend + the snapshot SQLite DB
# baked in. No background scrapes, no cron, no eBay creds needed at runtime
# — this image is a read-only demo of today's data state.

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps: sqlite ships as part of python, but the LightGBM wheel wants libgomp.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first so Docker layer-caches them across code changes.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app. .dockerignore excludes the 1.9GB data/cache/ directory and
# other development artifacts; the 233MB data/pokemon.db is included.
COPY . .

# HuggingFace Spaces expects the web server to listen on port 7860.
ENV PORT=7860
EXPOSE 7860

# uvicorn runs api.main:app — the FastAPI app serves both /api/* endpoints
# and the static frontend under the same origin.
CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-7860}"]
