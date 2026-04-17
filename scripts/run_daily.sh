#!/bin/bash
# run_daily.sh -- thin wrapper that cron invokes for the daily pipeline.
#
# Derives the project dir from the script's own location (no hardcoded
# sibling paths). Activates .venv/ if present. Tees stdout/stderr to a
# dated log. Emits a failure banner on non-zero exit so cron MAILTO
# captures it; the pipeline itself also writes to data/logs/alerts.jsonl
# and optionally POSTs to ALERT_WEBHOOK_URL.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_DIR}/data/logs"
VENV_ACTIVATE="${PROJECT_DIR}/.venv/bin/activate"

mkdir -p "${LOG_DIR}"

LOG_FILE="${LOG_DIR}/run_daily_$(date +%Y-%m-%d).log"

cd "${PROJECT_DIR}"

if [[ -f "${VENV_ACTIVATE}" ]]; then
    # shellcheck disable=SC1090
    source "${VENV_ACTIVATE}"
    PY="python"
else
    PY="python3"
fi

{
    echo "==================================================="
    echo "Delta Dex :: daily :: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Project: ${PROJECT_DIR}"
    echo "Python:  $(${PY} --version 2>&1)"
    echo "Args:    $*"
    echo "==================================================="
} | tee -a "${LOG_FILE}"

START_TS=$(date +%s)
set +e
"${PY}" -m pipeline.daily_pipeline "$@" 2>&1 | tee -a "${LOG_FILE}"
EXIT_CODE=${PIPESTATUS[0]}
set -e
END_TS=$(date +%s)
DURATION=$((END_TS - START_TS))

{
    echo "---------------------------------------------------"
    echo "Exit code: ${EXIT_CODE}"
    echo "Duration:  ${DURATION}s"
    echo "==================================================="
} | tee -a "${LOG_FILE}"

if [[ "${EXIT_CODE}" -ne 0 ]]; then
    {
        echo ""
        echo "!!! DELTA DEX DAILY PIPELINE FAILURE !!!"
        echo "Exit code: ${EXIT_CODE}"
        echo "Log file:  ${LOG_FILE}"
        echo "Alerts:    ${LOG_DIR}/alerts.jsonl"
        echo "Time:      $(date '+%Y-%m-%d %H:%M:%S')"
        echo ""
    } >&2
fi

exit "${EXIT_CODE}"
