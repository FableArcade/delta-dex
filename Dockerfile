# Delta Dex — Production deploy with Postgres + cron scrapers.

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 libpq5 cron supervisor curl gzip \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /app/data

# Cron wrapper — sources env vars before running Python
RUN echo '#!/bin/bash\ncd /app\nsource /etc/environment.sh 2>/dev/null\nexec /usr/local/bin/python "$@"\n' > /app/cron-run.sh && chmod +x /app/cron-run.sh

# Supervisor config
RUN echo "[supervisord]\n\
nodaemon=true\n\
logfile=/tmp/supervisord.log\n\
pidfile=/tmp/supervisord.pid\n\
\n\
[program:uvicorn]\n\
command=/usr/local/bin/uvicorn api.main:app --host 0.0.0.0 --port %(ENV_PORT)s\n\
directory=/app\n\
autostart=true\n\
autorestart=true\n\
stdout_logfile=/dev/stdout\n\
stdout_logfile_maxbytes=0\n\
stderr_logfile=/dev/stderr\n\
stderr_logfile_maxbytes=0\n\
environment=PYTHONPATH=/app,DATABASE_URL="%(ENV_DATABASE_URL)s",EBAY_APP_ID="%(ENV_EBAY_APP_ID)s",EBAY_CERT_ID="%(ENV_EBAY_CERT_ID)s"\n\
\n\
[program:cron]\n\
command=/usr/sbin/cron -f\n\
autostart=true\n\
autorestart=true\n\
stdout_logfile=/dev/stdout\n\
stdout_logfile_maxbytes=0\n\
stderr_logfile=/dev/stderr\n\
stderr_logfile_maxbytes=0\n" \
    > /etc/supervisor/conf.d/pokedelta.conf

# Startup script — same inline pattern as the working old deploy
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
mkdir -p /tmp/logs\n\
mkdir -p /app/data\n\
\n\
# Download SQLite as fallback if Postgres fails\n\
if [ ! -f /app/data/pokemon.db ]; then\n\
    curl -fSL https://github.com/FableArcade/delta-dex/releases/download/v0.2.0/pokemon.db.gz -o /tmp/pokemon.db.gz 2>/dev/null && \\\n\
    gunzip -c /tmp/pokemon.db.gz > /app/data/pokemon.db && \\\n\
    rm -f /tmp/pokemon.db.gz && \\\n\
    echo "SQLite fallback ready" || echo "SQLite download failed (non-fatal)"\n\
fi\n\
\n\
# Dump env vars for cron\n\
printenv > /etc/environment.sh 2>/dev/null || true\n\
sed -i "s/^/export /" /etc/environment.sh\n\
\n\
# Generate crontab\n\
echo "15 0 * * * root /app/cron-run.sh -m scripts.populate_ebay_signal_universe >> /tmp/logs/cron_ebay.log 2>&1" > /etc/cron.d/deltadex\n\
echo "0 10 * * * root /app/cron-run.sh -m scripts.populate_ebay_signal_universe >> /tmp/logs/cron_ebay.log 2>&1" >> /etc/cron.d/deltadex\n\
echo "30 10 * * * root /app/cron-run.sh -m scripts.populate_ebay_dip_candidates >> /tmp/logs/cron_ebay.log 2>&1" >> /etc/cron.d/deltadex\n\
echo "0 11 * * * root /app/cron-run.sh -m pipeline.daily_pipeline >> /tmp/logs/cron_daily.log 2>&1" >> /etc/cron.d/deltadex\n\
echo "" >> /etc/cron.d/deltadex\n\
chmod 0644 /etc/cron.d/deltadex\n\
\n\
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/pokedelta.conf\n' \
    > /app/start.sh \
    && chmod +x /app/start.sh

ENV PORT=7860
EXPOSE 7860

CMD ["/app/start.sh"]
