# Research Options Bot for LEAN

This folder contains the LEAN-side strategy skeleton for the local Docker path.

The intended flow is:

1. Run the app research scan.
2. Export `runtime/lean_watchlist.json`.
3. Mount/copy that file into the LEAN runtime.
4. Run `ResearchOptionsBot` in dry-run mode first.
5. Enable Alpaca paper execution only after logs, risk checks, and contract selection behave as expected.

The strategy is intentionally conservative:

- It only watches tickers selected by the research/options pipeline.
- It trades options only.
- It confirms direction with trend/RSI/MACD checks inside LEAN.
- It starts with `execute_orders = false`, so it logs candidates instead of placing orders.

## Export Watchlist

From the repo root:

```bash
venv/bin/python scripts/export_lean_watchlist.py
```

The export is written to:

```text
runtime/lean_watchlist.json
```

## Pull LEAN Docker Image

```bash
bash scripts/pull_lean_image.sh
```

## Dry Load in Docker

```bash
bash scripts/run_lean_dry_check.sh
```

This confirms the LEAN container can import the Python strategy and read the exported research candidates. The stock/options data itself is not bundled for the current watchlist symbols, so this check validates integration shape, not strategy performance.

## Full Local Dry Stack

```bash
bash scripts/run_full_dry_stack.sh
```

This runs Alpaca auth, research export, and LEAN dry load in sequence. Orders remain disabled.

Using LEAN CLI is still the smoothest local runner if you choose a paid QuantConnect organization later. The strategy in `main.py` is written so it can be copied into a LEAN CLI project or a QuantConnect project.
