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
# Times are UTC. Env vars are injected at startup by start.sh.
# The crontab is generated at runtime (not build time) so it
# picks up the actual EBAY_APP_ID etc from Railway's env vars.
RUN echo '#!/bin/bash\n\
cd /app\n\
source /etc/environment.sh\n\
exec /usr/local/bin/python "$@"\n' > /app/cron-run.sh && chmod +x /app/cron-run.sh

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
# Dump ALL env vars so cron scripts can source them\n\
printenv > /etc/environment.sh 2>/dev/null || true\n\
sed -i "s/^/export /" /etc/environment.sh\n\
\n\
# Generate crontab at runtime with env-aware wrapper\n\
echo "# Delta Dex cron — generated at startup" > /etc/cron.d/deltadex\n\
echo "15 0 * * * root /app/cron-run.sh -m scripts.populate_ebay_signal_universe >> /data/logs/cron_ebay.log 2>&1 && /app/cron-run.sh -m scripts.populate_ebay_dip_candidates >> /data/logs/cron_ebay.log 2>&1 && /app/cron-run.sh -m pipeline.daily_pipeline >> /data/logs/cron_daily.log 2>&1" >> /etc/cron.d/deltadex\n\
echo "0 10 * * * root /app/cron-run.sh -m scripts.populate_ebay_signal_universe >> /data/logs/cron_ebay.log 2>&1" >> /etc/cron.d/deltadex\n\
echo "30 10 * * * root /app/cron-run.sh -m scripts.populate_ebay_dip_candidates >> /data/logs/cron_ebay.log 2>&1" >> /etc/cron.d/deltadex\n\
echo "0 11 * * * root /app/cron-run.sh -m pipeline.daily_pipeline >> /data/logs/cron_daily.log 2>&1" >> /etc/cron.d/deltadex\n\
echo "0 9 * * 0 root /app/cron-run.sh -m pipeline.daily_pipeline --stage compute >> /data/logs/cron_weekly.log 2>&1" >> /etc/cron.d/deltadex\n\
echo "" >> /etc/cron.d/deltadex\n\
chmod 0644 /etc/cron.d/deltadex\n\
\n\
# Run eBay collection + pipeline immediately on first boot\n\
echo "Running initial data collection..."\n\
/app/cron-run.sh -m scripts.populate_ebay_signal_universe >> /data/logs/cron_ebay.log 2>&1 &\n\
\n\
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/pokedelta.conf\n' \
    > /app/start.sh \
    && chmod +x /app/start.sh

ENV PORT=7860
EXPOSE 7860

CMD ["/app/start.sh"]
