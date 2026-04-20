#!/bin/bash
mkdir -p /tmp/logs

# Dump env vars for cron jobs
env > /etc/environment.sh
sed -i 's/^/export /' /etc/environment.sh

# Setup cron
cat > /etc/cron.d/deltadex << 'CRONTAB'
15 0 * * * root /app/cron-run.sh -m scripts.populate_ebay_signal_universe >> /tmp/logs/cron_ebay.log 2>&1
0 10 * * * root /app/cron-run.sh -m scripts.populate_ebay_signal_universe >> /tmp/logs/cron_ebay.log 2>&1
30 10 * * * root /app/cron-run.sh -m scripts.populate_ebay_dip_candidates >> /tmp/logs/cron_ebay.log 2>&1
0 11 * * * root /app/cron-run.sh -m pipeline.daily_pipeline >> /tmp/logs/cron_daily.log 2>&1

CRONTAB
chmod 0644 /etc/cron.d/deltadex
cron

# SQLite fallback
if [ ! -f /app/data/pokemon.db ]; then
    curl -fSL https://github.com/FableArcade/delta-dex/releases/download/v0.2.0/pokemon.db.gz -o /tmp/pokemon.db.gz 2>/dev/null && \
    gunzip -c /tmp/pokemon.db.gz > /app/data/pokemon.db && \
    rm -f /tmp/pokemon.db.gz && \
    echo "SQLite fallback ready" || echo "SQLite download skipped"
fi

echo "DB: DATABASE_URL=${DATABASE_URL:+SET}${DATABASE_URL:-NOT SET}"
python3 -c "import os; print('PYTHON sees DATABASE_URL:', 'YES' if os.environ.get('DATABASE_URL') else 'NO')"

exec uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-7860}
