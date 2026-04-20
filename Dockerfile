# Delta Dex — Production deploy with Postgres + cron scrapers.

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 libpq5 cron curl gzip \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /app/data \
    && chmod +x /app/scripts/start.sh \
    && echo '#!/bin/bash\ncd /app\nsource /etc/environment.sh 2>/dev/null\nexec /usr/local/bin/python "$@"' > /app/cron-run.sh \
    && chmod +x /app/cron-run.sh

ENV PORT=7860
EXPOSE 7860

CMD ["/app/scripts/start.sh"]
