#!/bin/bash
set -e

mkdir -p /tmp/logs

echo "DB: DATABASE_URL is ${DATABASE_URL:+SET}${DATABASE_URL:-NOT SET}"

# Dump env vars for cron
printenv > /etc/environment.sh 2>/dev/null || true
sed -i 's/^/export /' /etc/environment.sh 2>/dev/null || true

# Generate crontab
cat > /etc/cron.d/deltadex << 'CRONTAB'
15 0 * * * root /app/cron-run.sh -m scripts.populate_ebay_signal_universe >> /tmp/logs/cron_ebay.log 2>&1 && /app/cron-run.sh -m scripts.populate_ebay_dip_candidates >> /tmp/logs/cron_ebay.log 2>&1 && /app/cron-run.sh -m pipeline.daily_pipeline >> /tmp/logs/cron_daily.log 2>&1
0 10 * * * root /app/cron-run.sh -m scripts.populate_ebay_signal_universe >> /tmp/logs/cron_ebay.log 2>&1
30 10 * * * root /app/cron-run.sh -m scripts.populate_ebay_dip_candidates >> /tmp/logs/cron_ebay.log 2>&1
0 11 * * * root /app/cron-run.sh -m pipeline.daily_pipeline >> /tmp/logs/cron_daily.log 2>&1
0 9 * * 0 root /app/cron-run.sh -m pipeline.daily_pipeline --stage compute >> /tmp/logs/cron_weekly.log 2>&1

CRONTAB
chmod 0644 /etc/cron.d/deltadex

exec /usr/bin/supervisord -c /etc/supervisor/conf.d/pokedelta.conf
