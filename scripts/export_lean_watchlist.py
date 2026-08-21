#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from agents import OmniRouteClient, ResearcherAgent
from lean_adapter import DEFAULT_LEAN_EXPORT_PATH, write_lean_universe


def main() -> int:
    researcher = ResearcherAgent(OmniRouteClient())
    research = researcher.scan_market()
    output_path = write_lean_universe(research, ROOT / DEFAULT_LEAN_EXPORT_PATH)
    candidates = len(research.get("watchlist", []))
    print(f"Wrote {candidates} LEAN candidates to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

