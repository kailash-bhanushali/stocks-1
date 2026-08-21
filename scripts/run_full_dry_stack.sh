#!/usr/bin/env bash
set -euo pipefail

echo "== Alpaca paper account =="
venv/bin/python scripts/check_alpaca_paper.py

echo
echo "== Research export for LEAN =="
venv/bin/python scripts/export_lean_watchlist.py

echo
echo "== LEAN Docker dry load =="
bash scripts/run_lean_dry_check.sh | grep -E 'Selected /workspace|Loaded [0-9]+ research candidates|Failed data requests percentage|Program.Main\(\): Exiting Lean' || true

echo
echo "Dry stack complete. Orders remain disabled unless ALPACA_TRADING_ENABLED=true and the LEAN strategy is run with execute_orders=true."

