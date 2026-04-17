# Delta Dex — Production deploy with scrapers + cron.
#
# Runs FastAPI web server + cron scrapers in a single container.
# SQLite DB should live on a persistent volume mounted at /data.

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps: libgomp for LightGBM, cron for scheduled scrapers,
# supervisor to run both uvicorn + cron in one container,
# curl + gzip to download DB on first boot.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 cron supervisor curl gzip \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# App code (data/ excluded via .dockerignore — DB downloads on first boot)
COPY . .
RUN mkdir -p /app/data

# Cron schedule — scrapers run on the server, not localhost.
# Times are UTC. Adjust if needed.
RUN echo "# eBay async collection (signal universe + dip candidates)\n\
0 10 * * * cd /app && /usr/local/bin/python -m scripts.populate_ebay_signal_universe >> /data/logs/cron_ebay.log 2>&1\n\
30 10 * * * cd /app && /usr/local/bin/python -m scripts.populate_ebay_dip_candidates >> /data/logs/cron_ebay_dip.log 2>&1\n\
# Daily pipeline (prices + signals + model)\n\
0 11 * * * cd /app && /usr/local/bin/python -m pipeline.daily_pipeline >> /data/logs/cron_daily.log 2>&1\n\
# Weekly pipeline\n\
0 9 * * 0 cd /app && /usr/local/bin/python -m pipeline.daily_pipeline --stage compute >> /data/logs/cron_weekly.log 2>&1\n" \
    > /etc/cron.d/pokedelta \
    && chmod 0644 /etc/cron.d/pokedelta \
    && crontab /etc/cron.d/pokedelta

# Supervisor config — runs uvicorn + cron side by side
RUN echo "[supervisord]\n\
nodaemon=true\n\
logfile=/data/logs/supervisord.log\n\
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
environment=PYTHONPATH=/app\n\
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

# Startup script: ensure /data dirs exist, symlink DB if on persistent volume,
# pass env vars to cron, then start supervisor.
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Ensure log dir exists on persistent volume\n\
mkdir -p /data/logs\n\
\n\
# If DB exists on persistent volume, use it. Otherwise download from GitHub release.\n\
if [ -f /data/pokemon.db ]; then\n\
    ln -sf /data/pokemon.db /app/data/pokemon.db\n\
    echo "Using persistent DB at /data/pokemon.db"\n\
else\n\
    echo "Downloading DB snapshot from GitHub release..."\n\
    curl -fSL https://github.com/FableArcade/delta-dex/releases/download/v0.1.0/pokemon.db.gz -o /tmp/pokemon.db.gz\n\
    gunzip -c /tmp/pokemon.db.gz > /data/pokemon.db\n\
    rm -f /tmp/pokemon.db.gz\n\
    ln -sf /data/pokemon.db /app/data/pokemon.db\n\
    echo "Seeded persistent DB from GitHub release ($(du -h /data/pokemon.db | cut -f1))"\n\
fi\n\
\n\
# Pass env vars to cron (cron runs in a clean env by default)\n\
printenv | grep -E "^(EBAY_|PRICECHARTING_|TCGPLAYER_|DATABASE_|ALERT_|PORT)" \\\n\
    > /etc/environment 2>/dev/null || true\n\
\n\
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/pokedelta.conf\n' \
    > /app/start.sh \
    && chmod +x /app/start.sh

ENV PORT=7860
EXPOSE 7860

CMD ["/app/start.sh"]
