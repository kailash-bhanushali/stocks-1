#!/usr/bin/env bash
set -euo pipefail

docker run --rm \
  -v "$PWD:/workspace" \
  quantconnect/lean:latest \
  --algorithm-type-name ResearchOptionsBot \
  --algorithm-language Python \
  --algorithm-location /workspace/lean/research_options_bot/main.py \
  --data-folder /Lean/Data \
  --close-automatically true \
  --parameters "research_file=/workspace/runtime/lean_watchlist.json,execute_orders=false"

