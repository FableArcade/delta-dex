#!/bin/bash
# refresh_new_signals.sh -- daily refresh for Signal Classes 3+4
#
# Runs outside the main daily_pipeline so it can fail independently
# without affecting the core price + scoring pipeline.
#
# 1. eBay liquidity collector (2000 API calls, ~7 min)
# 2. Tournament top-cut collector (~33 req total, ~1 min)
#
# Cron: run nightly at 03:30 local (before main pipeline at 04:00)
#   30 3 * * * /path/to/scripts/refresh_new_signals.sh >> logs

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_DIR}/data/logs"
VENV_ACTIVATE="${PROJECT_DIR}/.venv/bin/activate"

mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/refresh_signals_$(date +%Y-%m-%d).log"

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
    echo "Signal refresh :: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "==================================================="
} | tee -a "${LOG_FILE}"

# --- Tournament top-cut collector (fast, ~1 min) ---
echo "--- Tournament collector ---" | tee -a "${LOG_FILE}"
set +e
"${PY}" -m pipeline.collectors.tournaments 2>&1 | tee -a "${LOG_FILE}"
TOUR_EXIT=${PIPESTATUS[0]}
set -e
echo "Tournament collector exit=${TOUR_EXIT}" | tee -a "${LOG_FILE}"

# --- eBay liquidity collector (slow, ~7 min) ---
echo "--- eBay liquid-universe collector ---" | tee -a "${LOG_FILE}"
set +e
"${PY}" -m scripts.populate_ebay_liquid 2>&1 | tee -a "${LOG_FILE}"
EBAY_EXIT=${PIPESTATUS[0]}
set -e
echo "eBay collector exit=${EBAY_EXIT}" | tee -a "${LOG_FILE}"

{
    echo "==================================================="
    echo "Signal refresh done :: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "==================================================="
} | tee -a "${LOG_FILE}"

# Fail only if BOTH failed — partial success is still useful
if [[ "${TOUR_EXIT}" -ne 0 && "${EBAY_EXIT}" -ne 0 ]]; then
    echo "!!! Both collectors failed !!!" >&2
    exit 1
fi
exit 0
