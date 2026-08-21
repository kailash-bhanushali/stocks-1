from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_LEAN_EXPORT_PATH = Path("runtime/lean_watchlist.json")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_lean_universe(research: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert the app research payload into a small LEAN-readable contract.
    This file is intentionally secret-free and can be mounted into a LEAN
    container or copied into a QuantConnect project.
    """
    candidates: List[Dict[str, Any]] = []
    for item in research.get("watchlist", []):
        contract = (item.get("options") or {}).get("selected_contract") or {}
        if not contract.get("symbol"):
            continue

        bias = item.get("bias") or contract.get("type")
        candidates.append({
            "ticker": item.get("ticker"),
            "theme": item.get("theme"),
            "bias": bias,
            "action": "buy_call" if bias == "call" else "buy_put",
            "score": item.get("score"),
            "theme_score": item.get("theme_score"),
            "technical_score": item.get("technical_score"),
            "fundamental_score": item.get("fundamental_score"),
            "price": item.get("price"),
            "trigger": item.get("trigger"),
            "confirmation": item.get("confirmation", []),
            "risk_notes": item.get("risk_notes", []),
            "option_contract": {
                "symbol": contract.get("symbol"),
                "type": contract.get("type"),
                "expiration": contract.get("expiration"),
                "strike": contract.get("strike"),
                "dte": contract.get("dte"),
                "premium": contract.get("premium"),
                "spread_pct": contract.get("spread_pct"),
                "open_interest": contract.get("open_interest"),
                "volume": contract.get("volume"),
                "delta": contract.get("delta"),
                "iv": contract.get("iv"),
            },
        })

    risk = (research.get("debate") or {}).get("risk") or {}
    return {
        "schema_version": "lean-watchlist-v1",
        "generated_at": utc_now(),
        "research_run_id": research.get("run_id"),
        "research_generated_at": research.get("generated_at"),
        "mode": "dry_run_until_paper_enabled",
        "risk": {
            "market_regime": risk.get("market_regime"),
            "max_option_premium": risk.get("max_option_premium"),
            "spy_trend_score": risk.get("spy_trend_score"),
            "qqq_trend_score": risk.get("qqq_trend_score"),
        },
        "themes": [
            {
                "name": theme.get("name"),
                "strength": theme.get("strength"),
                "direction": theme.get("direction"),
            }
            for theme in research.get("themes", [])[:5]
        ],
        "candidates": candidates,
    }


def write_lean_universe(
    research: Dict[str, Any],
    output_path: Path = DEFAULT_LEAN_EXPORT_PATH,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_lean_universe(research)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path

