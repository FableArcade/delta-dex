#!/bin/bash
# run_ebay_async.sh -- async eBay collection for signal universe + dip candidates
#
# Runs the fast async collector (~3 min for 2000 cards) then recomputes
# market_pressure + supply_saturation signals.
#
# Cron: run nightly at 03:00 local (before refresh_new_signals at 03:30)
#   0 3 * * * /path/to/scripts/run_ebay_async.sh >> logs

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_DIR}/data/logs"
VENV_ACTIVATE="${PROJECT_DIR}/.venv/bin/activate"

mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/ebay_async_$(date +%Y-%m-%d).log"

cd "${PROJECT_DIR}"
if [[ -f "${VENV_ACTIVATE}" ]]; then
    source "${VENV_ACTIVATE}"
    PY="python"
else
    PY="python3"
fi

{
    echo "==================================================="
    echo "eBay async collection :: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "==================================================="
} | tee -a "${LOG_FILE}"

# 1. Signal universe (cards with existing market data)
echo "--- Signal universe ---" | tee -a "${LOG_FILE}"
set +e
"${PY}" -m scripts.populate_ebay_signal_universe 2>&1 | tee -a "${LOG_FILE}"
SIG_EXIT=${PIPESTATUS[0]}
set -e
echo "Signal universe exit=${SIG_EXIT}" | tee -a "${LOG_FILE}"

# 2. Dip candidates (cards 20%+ off ATH, not yet collected today)
echo "--- Dip candidates ---" | tee -a "${LOG_FILE}"
set +e
"${PY}" -m scripts.populate_ebay_dip_candidates 2>&1 | tee -a "${LOG_FILE}"
DIP_EXIT=${PIPESTATUS[0]}
set -e
echo "Dip candidates exit=${DIP_EXIT}" | tee -a "${LOG_FILE}"

{
    echo "==================================================="
    echo "eBay async done :: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Signal exit=${SIG_EXIT}, Dip exit=${DIP_EXIT}"
    echo "==================================================="
} | tee -a "${LOG_FILE}"

exit 0
