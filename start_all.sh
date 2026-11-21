#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_BIN="${PY_BIN:-}"
if [[ -z "${PY_BIN}" ]]; then
  if [[ -x "${ROOT_DIR}/.venv/bin/python" ]]; then
    PY_BIN="${ROOT_DIR}/.venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PY_BIN="$(command -v python3)"
  else
    echo "❌ Python 3 not found. Create a venv and run: pip install -r requirements.txt"
    exit 1
  fi
fi

MODE="--paper"
BET_PCT="0.10"
MAX_LOSS_PCT="0.30"
BANKROLL="40"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --live)
      MODE="--live"
      shift
      ;;
    --paper)
      MODE="--paper"
      shift
      ;;
    --bet-pct)
      BET_PCT="$2"
      shift 2
      ;;
    --max-daily-loss-pct)
      MAX_LOSS_PCT="$2"
      shift 2
      ;;
    --bankroll)
      BANKROLL="$2"
      shift 2
      ;;
    *)
      echo "Usage: $0 [--paper | --live] [--bet-pct 0.10] [--max-daily-loss-pct 0.30] [--bankroll 40]"
      exit 2
      ;;
  esac
done

mkdir -p "${ROOT_DIR}/logs"
PIDS=()

cleanup() {
  echo ""
  echo "Stopping BTC / ETH / SOL monitors..."
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup INT TERM

EXTRA_FLAGS=("--bet" "0" "--bet-pct" "${BET_PCT}" "--max-daily-loss-pct" "${MAX_LOSS_PCT}")
if [[ "${MODE}" == "--live" ]]; then
  EXTRA_FLAGS=("--live" "${EXTRA_FLAGS[@]}")
  echo "🔴 Starting LIVE mode (--bet-pct ${BET_PCT}, --max-daily-loss-pct ${MAX_LOSS_PCT})"
else
  EXTRA_FLAGS=("--bankroll" "${BANKROLL}" "${EXTRA_FLAGS[@]}")
  echo "🟢 Starting PAPER simulation mode (bankroll=${BANKROLL}, --bet-pct ${BET_PCT})"
fi

(cd "${ROOT_DIR}/polymarket-btc-5m" && exec "${PY_BIN}" -u run_monitor.py "${EXTRA_FLAGS[@]}" >> "${ROOT_DIR}/logs/btc.log" 2>&1) &
PIDS+=("$!")
sleep 3

(cd "${ROOT_DIR}/polymarket-eth-5m" && exec "${PY_BIN}" -u run_monitor.py "${EXTRA_FLAGS[@]}" >> "${ROOT_DIR}/logs/eth.log" 2>&1) &
PIDS+=("$!")

(cd "${ROOT_DIR}/polymarket-sol-5m" && exec "${PY_BIN}" -u run_monitor.py "${EXTRA_FLAGS[@]}" >> "${ROOT_DIR}/logs/sol.log" 2>&1) &
PIDS+=("$!")

echo "✅ Started BTC (${PIDS[0]}), ETH (${PIDS[1]}), SOL (${PIDS[2]}). Logs in ${ROOT_DIR}/logs/."
echo "Press Ctrl+C to stop all three runners."
wait
