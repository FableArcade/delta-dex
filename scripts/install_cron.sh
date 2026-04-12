#!/bin/bash
# install_cron.sh -- install daily & weekly cron jobs for Pokemon Analytics.
#
# Idempotent: reads existing crontab, only adds entries that aren't already
# present, and writes the result back.
#
# Daily:  every day at 03:00 local time  -> pipeline.daily_pipeline
# Weekly: Sunday at 02:00 local time     -> pipeline.weekly_pipeline
#
# Logs are written to data/logs/.

set -euo pipefail

PROJECT_DIR="/Users/yoson/pokemon-analytics"
LOG_DIR="${PROJECT_DIR}/data/logs"
RUN_SCRIPT="${PROJECT_DIR}/scripts/run_pipeline.sh"

DAILY_TAG="# pokemon-analytics:daily"
WEEKLY_TAG="# pokemon-analytics:weekly"

DAILY_LINE="0 3 * * * ${RUN_SCRIPT} daily >> ${LOG_DIR}/cron_daily.log 2>&1 ${DAILY_TAG}"
WEEKLY_LINE="0 2 * * 0 ${RUN_SCRIPT} weekly >> ${LOG_DIR}/cron_weekly.log 2>&1 ${WEEKLY_TAG}"

# Ensure prerequisites exist
mkdir -p "${LOG_DIR}"

if [[ ! -x "${RUN_SCRIPT}" ]]; then
    echo "Making ${RUN_SCRIPT} executable..."
    chmod +x "${RUN_SCRIPT}" 2>/dev/null || true
fi

# Pull current crontab (empty string if none)
CURRENT_CRON="$(crontab -l 2>/dev/null || true)"

NEW_CRON="${CURRENT_CRON}"
ADDED=0

append_if_missing() {
    local tag="$1"
    local line="$2"
    if printf '%s\n' "${NEW_CRON}" | grep -Fq "${tag}"; then
        echo "  [skip] ${tag} already installed"
    else
        if [[ -n "${NEW_CRON}" ]]; then
            NEW_CRON="${NEW_CRON}"$'\n'"${line}"
        else
            NEW_CRON="${line}"
        fi
        ADDED=$((ADDED + 1))
        echo "  [add]  ${line}"
    fi
}

echo "Installing Pokemon Analytics cron jobs..."
append_if_missing "${DAILY_TAG}" "${DAILY_LINE}"
append_if_missing "${WEEKLY_TAG}" "${WEEKLY_LINE}"

if [[ "${ADDED}" -gt 0 ]]; then
    # Ensure trailing newline for crontab
    printf '%s\n' "${NEW_CRON}" | crontab -
    echo "Installed ${ADDED} new cron job(s)."
else
    echo "Nothing to install -- cron already up to date."
fi

echo
echo "Current pokemon-analytics crontab entries:"
crontab -l 2>/dev/null | grep "pokemon-analytics" || echo "  (none found)"
