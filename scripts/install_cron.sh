#!/bin/bash
# install_cron.sh -- install the daily Pokemon Analytics cron job.
#
# Idempotent: reads existing crontab, only appends entries that aren't
# already present (matched by the trailing tag comment), and writes back.
#
# Daily:  every day at 04:00 local time  -> scripts/run_daily.sh
# Weekly: Sunday at 02:00 local time     -> scripts/run_pipeline.sh weekly
#
# Logs go to data/logs/.

set -euo pipefail

# Resolve project dir from this script's location (no hardcoded sibling paths).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_DIR}/data/logs"

DAILY_SCRIPT="${PROJECT_DIR}/scripts/run_daily.sh"
WEEKLY_SCRIPT="${PROJECT_DIR}/scripts/run_pipeline.sh"

DAILY_TAG="# pokemon-analytics:daily"
WEEKLY_TAG="# pokemon-analytics:weekly"

DAILY_LINE="0 4 * * * ${DAILY_SCRIPT} >> ${LOG_DIR}/cron_daily.log 2>&1 ${DAILY_TAG}"
WEEKLY_LINE="0 2 * * 0 ${WEEKLY_SCRIPT} weekly >> ${LOG_DIR}/cron_weekly.log 2>&1 ${WEEKLY_TAG}"

DRY_RUN=0
for arg in "$@"; do
    case "$arg" in
        --dry-run|-n) DRY_RUN=1 ;;
    esac
done

mkdir -p "${LOG_DIR}"

if [[ -f "${DAILY_SCRIPT}" && ! -x "${DAILY_SCRIPT}" ]]; then
    echo "Making ${DAILY_SCRIPT} executable..."
    chmod +x "${DAILY_SCRIPT}" 2>/dev/null || true
fi
if [[ -f "${WEEKLY_SCRIPT}" && ! -x "${WEEKLY_SCRIPT}" ]]; then
    chmod +x "${WEEKLY_SCRIPT}" 2>/dev/null || true
fi

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
echo "  project: ${PROJECT_DIR}"
append_if_missing "${DAILY_TAG}" "${DAILY_LINE}"
append_if_missing "${WEEKLY_TAG}" "${WEEKLY_LINE}"

if [[ "${DRY_RUN}" -eq 1 ]]; then
    echo
    echo "[DRY RUN] would install ${ADDED} new entry/entries. New crontab would be:"
    printf '%s\n' "${NEW_CRON}"
    exit 0
fi

if [[ "${ADDED}" -gt 0 ]]; then
    printf '%s\n' "${NEW_CRON}" | crontab -
    echo "Installed ${ADDED} new cron job(s)."
else
    echo "Nothing to install -- cron already up to date."
fi

echo
echo "Current pokemon-analytics crontab entries:"
crontab -l 2>/dev/null | grep "pokemon-analytics" || echo "  (none found)"
