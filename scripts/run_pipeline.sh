#!/bin/bash
# run_pipeline.sh -- wrapper that cron invokes.
#
# Usage:
#   run_pipeline.sh daily
#   run_pipeline.sh weekly
#   run_pipeline.sh daily --date 2026-04-09
#   run_pipeline.sh daily --stage compute
#
# Behavior:
#   * cd into project dir
#   * activate .venv if it exists
#   * run the requested pipeline, tee-ing stdout/stderr to data/logs/
#   * on failure, print a notification stub (swap for email/slack later)

set -uo pipefail

PROJECT_DIR="/Users/yoson/pokemon-analytics"
LOG_DIR="${PROJECT_DIR}/data/logs"
VENV_ACTIVATE="${PROJECT_DIR}/.venv/bin/activate"

mkdir -p "${LOG_DIR}"

MODE="${1:-daily}"
shift || true

case "${MODE}" in
    daily)
        MODULE="pipeline.daily_pipeline"
        LOG_FILE="${LOG_DIR}/run_daily_$(date +%Y-%m-%d).log"
        ;;
    weekly)
        MODULE="pipeline.weekly_pipeline"
        LOG_FILE="${LOG_DIR}/run_weekly_$(date +%Y-%m-%d).log"
        ;;
    *)
        echo "Usage: $0 {daily|weekly} [pipeline args...]"
        exit 64
        ;;
esac

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
    echo "Pokemon Analytics :: ${MODE} :: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Module: ${MODULE}"
    echo "Args:   $*"
    echo "CWD:    $(pwd)"
    echo "Python: $(${PY} --version 2>&1)"
    echo "==================================================="
} | tee -a "${LOG_FILE}"

START_TS=$(date +%s)
set +e
"${PY}" -m "${MODULE}" "$@" 2>&1 | tee -a "${LOG_FILE}"
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

notify_failure() {
    local code="$1"
    # TODO: replace with real email/slack notification.
    # For now we just print a banner to stderr so cron's MAILTO picks it up.
    {
        echo ""
        echo "!!! POKEMON ANALYTICS ${MODE^^} PIPELINE FAILURE !!!"
        echo "Exit code: ${code}"
        echo "Log file:  ${LOG_FILE}"
        echo "Time:      $(date '+%Y-%m-%d %H:%M:%S')"
        echo ""
    } >&2
}

if [[ "${EXIT_CODE}" -ne 0 ]]; then
    notify_failure "${EXIT_CODE}"
fi

exit "${EXIT_CODE}"
