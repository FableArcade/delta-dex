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
RUN mkdir -p /app/data

# Cron wrapper
RUN echo '#!/bin/bash\ncd /app\nsource /etc/environment.sh 2>/dev/null\nexec /usr/local/bin/python "$@"\n' > /app/cron-run.sh && chmod +x /app/cron-run.sh

# Startup: dump env for cron, start cron daemon, download SQLite fallback, run uvicorn
RUN echo '#!/bin/bash\n\
mkdir -p /tmp/logs\n\
\n\
# Dump env vars for cron jobs\n\
env > /etc/environment.sh\n\
sed -i "s/^/export /" /etc/environment.sh\n\
\n\
# Setup cron\n\
echo "15 0 * * * root /app/cron-run.sh -m scripts.populate_ebay_signal_universe >> /tmp/logs/cron_ebay.log 2>&1" > /etc/cron.d/deltadex\n\
echo "0 10 * * * root /app/cron-run.sh -m scripts.populate_ebay_signal_universe >> /tmp/logs/cron_ebay.log 2>&1" >> /etc/cron.d/deltadex\n\
echo "30 10 * * * root /app/cron-run.sh -m scripts.populate_ebay_dip_candidates >> /tmp/logs/cron_ebay.log 2>&1" >> /etc/cron.d/deltadex\n\
echo "0 11 * * * root /app/cron-run.sh -m pipeline.daily_pipeline >> /tmp/logs/cron_daily.log 2>&1" >> /etc/cron.d/deltadex\n\
echo "" >> /etc/cron.d/deltadex\n\
chmod 0644 /etc/cron.d/deltadex\n\
cron\n\
\n\
# SQLite fallback\n\
if [ ! -f /app/data/pokemon.db ]; then\n\
    curl -fSL https://github.com/FableArcade/delta-dex/releases/download/v0.2.0/pokemon.db.gz -o /tmp/pokemon.db.gz 2>/dev/null && \\\n\
    gunzip -c /tmp/pokemon.db.gz > /app/data/pokemon.db && \\\n\
    rm -f /tmp/pokemon.db.gz && \\\n\
    echo "SQLite fallback ready" || echo "SQLite download skipped"\n\
fi\n\
\n\
echo "DB: DATABASE_URL=${DATABASE_URL:+SET}${DATABASE_URL:-NOT SET}"\n\
\n\
python3 -c "import os; print(f\\"PYTHON_ENV: DATABASE_URL={os.environ.get(\\'DATABASE_URL\\', \\'NOT SET\\')}\\", flush=True)"\n\
exec uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-7860}\n' \
    > /app/start.sh \
    && chmod +x /app/start.sh

ENV PORT=7860
EXPOSE 7860

CMD ["/app/start.sh"]
