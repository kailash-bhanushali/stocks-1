from datetime import datetime, timezone
import asyncio
import json
import os
import shutil
import subprocess
import time
from typing import Any, Dict, Optional
import urllib.parse

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import uvicorn

from alpaca_service import alpaca_account_summary
from agents import OmniRouteClient, ResearcherAgent, TechnicalConfirmationAgent, TraderAgent
from data_feed_router import DataFeedRouter
from intelligence_agent import (
    fetch_all_intelligence,
    fetch_insider_trades,
    fetch_congress_trades,
    fetch_corporate_actions,
    fetch_ownership,
    fetch_seasonality,
    fetch_analyst_and_valuation,
)
from lean_adapter import build_lean_universe, write_lean_universe
from option_streamer import alpaca_option_stream

load_dotenv()

app = FastAPI(title="Research-First AI Trading Platform")
app.mount("/static", StaticFiles(directory="static"), name="static")


class TradingViewSignal(BaseModel):
    action: str
    ticker: str
    price: Optional[float] = None
    time: Optional[str] = None
    interval: Optional[str] = None
    indicator: Optional[str] = None
    strategy: Optional[str] = None
    option_symbol: Optional[str] = None
    signal_type: Optional[str] = None
    trend: Optional[str] = None
    source: str = "unknown"
    verified: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)


llm_client = OmniRouteClient()
researcher = ResearcherAgent(llm_client)
technical = TechnicalConfirmationAgent()
trader = TraderAgent(llm_client)
data_feeds = DataFeedRouter()

DEFAULT_MARKET_DIGEST = """
Optional operator notes can be supplied here. The scanner now uses live data
sources first and keeps this text only as additional context.
"""

PIPELINE_TEMPLATE = [
    {
        "id": "discovery",
        "label": "Discovery",
        "caption": "sector momentum scan",
        "status": "idle",
        "detail": "Waiting for scan"
    },
    {
        "id": "market",
        "label": "Market",
        "caption": "price, breadth, volume",
        "status": "idle",
        "detail": "Waiting for scan"
    },
    {
        "id": "sources",
        "label": "Sources",
        "caption": "news, social, fundamentals",
        "status": "idle",
        "detail": "Waiting for scan"
    },
    {
        "id": "research",
        "label": "Research Team",
        "caption": "bull/bear debate",
        "status": "idle",
        "detail": "No debate yet"
    },
    {
        "id": "options",
        "label": "Options",
        "caption": "liquidity validation",
        "status": "idle",
        "detail": "No contracts checked"
    },
    {
        "id": "themes",
        "label": "Themes",
        "caption": "rank active baskets",
        "status": "idle",
        "detail": "No themes ranked"
    },
    {
        "id": "watchlist",
        "label": "Watchlist",
        "caption": "option-validated",
        "status": "idle",
        "detail": "No candidates selected"
    },
    {
        "id": "datafeeds",
        "label": "Data Feeds",
        "caption": "live quote router",
        "status": "idle",
        "detail": "No quotes checked"
    },
    {
        "id": "optionstream",
        "label": "Option Stream",
        "caption": "Alpaca indicative",
        "status": "idle",
        "detail": "No contracts subscribed"
    },
    {
        "id": "lean",
        "label": "LEAN",
        "caption": "engine dry-run",
        "status": "idle",
        "detail": "No exported candidates"
    },
    {
        "id": "tradingview",
        "label": "TradingView",
        "caption": "indicator confirm",
        "status": "needs_setup",
        "detail": "No verified webhook received"
    },
    {
        "id": "decision",
        "label": "Decision",
        "caption": "risk + manager gate",
        "status": "idle",
        "detail": "No confirmed setup"
    },
    {
        "id": "execution",
        "label": "Execution",
        "caption": "Alpaca next phase",
        "status": "disabled",
        "detail": "Execution intentionally disabled"
    }
]

pipeline_state: Dict[str, Any] = {
    "mode": "research_first",
    "status": "booting",
    "updated_at": None,
    "stages": [],
    "research": {},
    "last_signal": None,
    "technical_confirmation": None,
    "last_decision": None,
    "last_execution": None,
    "tradingview": {
        "connected": False,
        "verified": False,
        "last_real_webhook_at": None,
        "secret_configured": bool(os.getenv("TRADINGVIEW_WEBHOOK_SECRET")),
    },
    "lean": {
        "watchlist_export_path": None,
        "candidate_count": 0,
        "mode": "dry_run_until_paper_enabled",
    },
    "alpaca": {
        "status": "unknown",
        "checked_at": None,
        "detail": "Not checked yet",
    },
    "data_feeds": {
        "status": "unknown",
        "checked_at": None,
        "detail": "Not checked yet",
        "snapshot": None,
    },
    "option_stream": alpaca_option_stream.snapshot(),
    "events": []
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def event(message: str, level: str = "info") -> None:
    pipeline_state["events"].insert(0, {
        "time": utc_now(),
        "level": level,
        "message": message
    })
    pipeline_state["events"] = pipeline_state["events"][:50]


def stage_status(stage_id: str, status: str, detail: str) -> None:
    for stage in pipeline_state["stages"]:
        if stage["id"] == stage_id:
            stage["status"] = status
            stage["detail"] = detail
            return


def _cache_is_fresh(payload: Dict[str, Any], max_age_seconds: int = 60) -> bool:
    checked_at = payload.get("checked_at")
    if not checked_at:
        return False
    try:
        checked = datetime.fromisoformat(checked_at)
    except ValueError:
        return False
    return (datetime.now(timezone.utc) - checked).total_seconds() <= max_age_seconds


def refresh_alpaca_status(force: bool = False) -> Dict[str, Any]:
    if not force and _cache_is_fresh(pipeline_state.get("alpaca", {}), 45):
        return pipeline_state["alpaca"]
    try:
        summary = alpaca_account_summary()
        blocked = summary.get("trading_blocked") or summary.get("account_blocked")
        options_level = summary.get("options_trading_level") or summary.get("options_approved_level")
        status = "blocked" if blocked else "ok"
        detail = f"Paper account {summary.get('account_status')}; options level {options_level}"
        payload = {
            **summary,
            "status": status,
            "checked_at": utc_now(),
            "detail": detail,
        }
    except Exception as exc:
        payload = {
            "status": "error",
            "checked_at": utc_now(),
            "detail": str(exc)[:220],
            "paper": os.getenv("ALPACA_PAPER", "true").lower() == "true",
            "trading_enabled": os.getenv("ALPACA_TRADING_ENABLED", "false").lower() == "true",
        }
    pipeline_state["alpaca"] = payload
    return payload


def lean_docker_status() -> Dict[str, Any]:
    if not shutil.which("docker"):
        return {
            "docker": "missing",
            "image": "missing",
            "detail": "Docker CLI not found",
        }
    try:
        result = subprocess.run(
            [
                "docker",
                "image",
                "inspect",
                "quantconnect/lean:latest",
                "--format",
                "{{.Id}}|{{.Architecture}}|{{.Size}}",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode != 0:
            return {
                "docker": "available",
                "image": "missing",
                "detail": "quantconnect/lean:latest not found",
            }
        image_id, architecture, size = (result.stdout.strip().split("|") + ["", "", ""])[:3]
        return {
            "docker": "available",
            "image": "installed",
            "image_id": image_id.replace("sha256:", "")[:12],
            "architecture": architecture,
            "size_gb": round(int(size or 0) / 1_000_000_000, 1),
            "detail": "LEAN Docker image installed",
        }
    except Exception as exc:
        return {
            "docker": "error",
            "image": "unknown",
            "detail": str(exc)[:220],
        }


def refresh_data_feed_status(force: bool = False) -> Dict[str, Any]:
    current = pipeline_state.get("data_feeds", {})
    if not force and _cache_is_fresh(current, 45):
        return current
    watchlist = pipeline_state.get("research", {}).get("watchlist", [])
    if not watchlist:
        payload = {
            "status": "waiting",
            "checked_at": utc_now(),
            "detail": "No research watchlist yet",
            "snapshot": None,
        }
        pipeline_state["data_feeds"] = payload
        return payload
    try:
        from agents import get_sources_config
        snapshot = data_feeds.snapshot(watchlist, get_sources_config()["market"])
        provider_counts = {
            "ok": sum(1 for item in snapshot["providers"] if item.get("status") == "ok"),
            "not_configured": sum(1 for item in snapshot["providers"] if item.get("status") == "not_configured"),
            "error": sum(1 for item in snapshot["providers"] if item.get("status") == "error"),
        }
        selected_underlyings = sum(1 for item in snapshot["underlyings"].values() if item.get("selected"))
        selected_options = sum(1 for item in snapshot["options"].values() if item.get("selected"))
        payload = {
            "status": "ok" if selected_underlyings or selected_options else "degraded",
            "checked_at": utc_now(),
            "detail": f"{selected_underlyings} stock quotes, {selected_options} option quotes selected",
            "provider_counts": provider_counts,
            "snapshot": snapshot,
        }
    except Exception as exc:
        payload = {
            "status": "error",
            "checked_at": utc_now(),
            "detail": str(exc)[:240],
            "snapshot": None,
        }
    pipeline_state["data_feeds"] = payload
    return payload


def watchlist_option_contracts(research: Dict[str, Any]) -> list:
    contracts = []
    for item in research.get("watchlist", []):
        symbol = (item.get("options") or {}).get("selected_contract", {}).get("symbol")
        if symbol:
            contracts.append(symbol)
    return contracts


def seed_option_stream_quotes(data_feed_status: Dict[str, Any]) -> None:
    snapshot = data_feed_status.get("snapshot") or {}
    seed_quotes = []
    for contract, quote_bundle in (snapshot.get("options") or {}).items():
        selected = (quote_bundle or {}).get("selected") or {}
        if selected and selected.get("provider") == "alpaca":
            seed_quotes.append({**selected, "symbol": contract})
    if seed_quotes:
        alpaca_option_stream.seed_quotes(seed_quotes)


def sync_option_stream_subscriptions(research: Dict[str, Any], data_feed_status: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    contracts = watchlist_option_contracts(research)
    if data_feed_status:
        seed_option_stream_quotes(data_feed_status)
    # An empty latest watchlist must also clear old subscriptions and quotes.
    status = alpaca_option_stream.set_symbols(contracts)
    pipeline_state["option_stream"] = status
    return status


def tradingview_stage_status() -> Dict[str, str]:
    if not os.getenv("TRADINGVIEW_WEBHOOK_SECRET"):
        return {"status": "needs_setup", "detail": "Set TRADINGVIEW_WEBHOOK_SECRET and expose HTTPS webhook"}
    if pipeline_state["tradingview"].get("verified"):
        return {"status": "waiting", "detail": "Connected; waiting for next verified watchlist alert"}
    if pipeline_state["tradingview"].get("connected"):
        return {"status": "blocked", "detail": "Webhook seen, but secret was not verified"}
    return {"status": "waiting", "detail": "Secret configured; waiting for first real webhook"}


def run_research_scan(market_digest: str = DEFAULT_MARKET_DIGEST) -> Dict[str, Any]:
    pipeline_state["stages"] = [stage.copy() for stage in PIPELINE_TEMPLATE]
    pipeline_state["status"] = "researching"
    stage_status("discovery", "running", "Scanning sector ETF momentum")
    stage_status("market", "running", "Loading price, trend, and volume metrics")
    stage_status("sources", "running", "Running news, social, and data-source agents")
    stage_status("research", "running", "Preparing bull/bear/risk debate")
    event("Dynamic discovery scan started: identifying active sectors before deep analysis.", "info")

    research = researcher.scan_market(market_digest)
    pipeline_state["research"] = research
    lean_export_path = write_lean_universe(research)
    lean_universe = build_lean_universe(research)
    pipeline_state["lean"] = {
        "watchlist_export_path": str(lean_export_path),
        "candidate_count": len(lean_universe["candidates"]),
        "mode": lean_universe["mode"],
        "generated_at": lean_universe["generated_at"],
        "docker": lean_docker_status(),
    }
    refresh_alpaca_status(force=True)
    data_feed_status = refresh_data_feed_status(force=True)
    option_stream_status = sync_option_stream_subscriptions(research, data_feed_status)
    pipeline_state["updated_at"] = utc_now()
    pipeline_state["last_signal"] = None
    pipeline_state["technical_confirmation"] = None
    pipeline_state["last_decision"] = None
    pipeline_state["last_execution"] = None

    top_theme = research["themes"][0] if research["themes"] else None
    top_tickers = ", ".join(item["ticker"] for item in research["watchlist"]) or "none"
    disc = research.get("discovery", {})
    stage_status(
        "discovery", "done",
        f"{disc.get('themes_active', '?')} active themes from {disc.get('sectors_active', '?')}/{disc.get('sectors_scanned', '?')} sectors"
    )
    stage_status("market", "done", f"{research['raw_inputs']['market']['symbols_loaded']} symbols loaded")
    stage_status("sources", "done", self_or_source_summary(research))
    stage_status("research", "done", research.get("debate", {}).get("manager", "Debate complete"))
    stage_status("options", "done" if research["watchlist"] else "blocked", f"Option-validated watchlist: {top_tickers}")
    stage_status("themes", "done" if top_theme else "blocked", f"{top_theme['name']} leads at {top_theme['strength']}/100" if top_theme else "No themes ranked")
    stage_status("watchlist", "done" if research["watchlist"] else "blocked", f"Watching {top_tickers}")
    stage_status(
        "datafeeds",
        "done" if data_feed_status["status"] == "ok" else data_feed_status["status"],
        data_feed_status["detail"],
    )
    stage_status(
        "optionstream",
        "done" if option_stream_status["status"] == "running" else option_stream_status["status"],
        option_stream_status["detail"],
    )
    stage_status("lean", "done" if lean_universe["candidates"] else "blocked", f"{len(lean_universe['candidates'])} candidates exported for LEAN dry-run")
    tv = tradingview_stage_status()
    stage_status("tradingview", tv["status"], tv["detail"])
    pipeline_state["status"] = "needs_setup" if tv["status"] == "needs_setup" else "watching"
    stage_status("execution", "disabled", "Alpaca execution is intentionally next phase")
    event(f"Top theme: {top_theme['name'] if top_theme else 'none'}. Watchlist: {top_tickers}.", "success" if research["watchlist"] else "warning")
    event(f"LEAN watchlist export refreshed with {pipeline_state['lean']['candidate_count']} candidates.", "info")
    event(f"Data feed router refreshed: {data_feed_status['detail']}.", "info")
    event(f"Alpaca option stream: {option_stream_status['detail']}.", "info")
    return research


def self_or_source_summary(research: Dict[str, Any]) -> str:
    health = research.get("source_health", [])
    ok = sum(1 for item in health if item.get("status") == "ok")
    degraded = sum(1 for item in health if item.get("status") == "degraded")
    unavailable = sum(1 for item in health if item.get("status") == "unavailable")
    return f"{ok} ok, {degraded} degraded, {unavailable} unavailable"


def tradingview_indicator_plan(item: Dict[str, Any]) -> Dict[str, Any]:
    bias = item.get("bias", "call")
    if bias == "call":
        signal_rule = "buy when weekly trend is bullish and daily breakout/continuation confirms"
        conditions = [
            "weekly close above 20 EMA and 50 EMA",
            "daily close above 20 EMA",
            "MACD histogram positive or crossing up",
            "RSI between 50 and 72",
            "relative volume above 1.2x or breakout above prior swing high",
        ]
    else:
        signal_rule = "sell when weekly trend is bearish and daily breakdown/continuation confirms"
        conditions = [
            "weekly close below 20 EMA and 50 EMA",
            "daily close below 20 EMA",
            "MACD histogram negative or crossing down",
            "RSI between 28 and 50",
            "relative volume above 1.2x or breakdown below prior swing low",
        ]
    contract = item.get("options", {}).get("selected_contract") or {}
    return {
        "ticker": item.get("ticker"),
        "theme": item.get("theme"),
        "bias": bias,
        "timeframes": ["1W", "1D"],
        "signal_rule": signal_rule,
        "conditions": conditions,
        "selected_option_contract": contract.get("symbol"),
        "contract_context": {
            "expiration": contract.get("expiration"),
            "strike": contract.get("strike"),
            "dte": contract.get("dte"),
            "premium": contract.get("premium"),
            "spread_pct": contract.get("spread_pct"),
            "open_interest": contract.get("open_interest"),
        },
        "alert_message_json": {
            "secret": "{{replace_with_TRADINGVIEW_WEBHOOK_SECRET}}",
            "ticker": "{{ticker}}",
            "action": "buy" if bias == "call" else "sell",
            "price": "{{close}}",
            "time": "{{time}}",
            "interval": "{{interval}}",
            "indicator": "research_theme_options_v1",
            "theme": item.get("theme"),
            "watchlist_score": item.get("score"),
            "option_symbol": contract.get("symbol"),
        }
    }


def nested_get(data: Dict[str, Any], *path: str) -> Any:
    current = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def infer_action(data: Dict[str, Any]) -> str:
    explicit = (
        data.get("action")
        or data.get("side")
        or data.get("signal")
        or nested_get(data, "strategy", "order_action")
    )
    if explicit:
        return str(explicit).lower()

    signal_type = str(data.get("signal_type") or "").upper()
    if signal_type in {"BUY", "LONG", "CALL"}:
        return "buy"
    if signal_type in {"SELL", "SHORT", "PUT"}:
        return "sell"

    trend = str(nested_get(data, "strategy", "trend") or data.get("trend") or "").lower()
    if trend == "bullish":
        return "buy"
    if trend == "bearish":
        return "sell"

    return "unknown"


def normalize_signal(data: Dict[str, Any], source: str, verified: bool) -> TradingViewSignal:
    strategy_payload = data.get("strategy") if isinstance(data.get("strategy"), dict) else {}
    normalized = {
        "action": infer_action(data),
        "ticker": data.get("ticker") or data.get("symbol"),
        "price": data.get("price") or data.get("close") or nested_get(data, "bar", "close"),
        "time": data.get("time") or data.get("timestamp"),
        "interval": data.get("interval") or data.get("timeframe"),
        "indicator": data.get("indicator") or data.get("signal_type") or data.get("strategy_name"),
        "strategy": data.get("strategy_name") or strategy_payload.get("name"),
        "option_symbol": data.get("option_symbol"),
        "signal_type": data.get("signal_type"),
        "trend": strategy_payload.get("trend") or data.get("trend"),
        "source": source,
        "verified": verified,
        "metadata": {
            key: value for key, value in data.items()
            if key not in {
                "action", "side", "signal", "ticker", "symbol", "price", "close",
                "time", "timestamp", "interval", "timeframe", "indicator",
                "strategy", "strategy_name", "option_symbol", "secret", "webhook_secret", "passphrase"
            }
        }
    }
    return TradingViewSignal(**normalized)


def verify_tradingview_secret(data: Dict[str, Any], request: Request) -> bool:
    expected = os.getenv("TRADINGVIEW_WEBHOOK_SECRET", "")
    supplied = (
        request.headers.get("x-tradingview-secret")
        or request.headers.get("x-webhook-secret")
        or data.get("secret")
        or data.get("webhook_secret")
        or data.get("passphrase")
        or ""
    )
    return bool(expected and supplied and supplied == expected)


def process_signal(signal: TradingViewSignal) -> Dict[str, Any]:
    if not pipeline_state["research"]:
        run_research_scan()

    signal_payload = signal.model_dump()
    pipeline_state["last_signal"] = signal_payload
    is_real_tv = signal.source == "tradingview" and signal.verified
    stage_status("tradingview", "running", f"{signal.ticker.upper()} {signal.action.upper()} received from {signal.source}")
    event(f"{signal.source.title()} signal received: {signal.ticker.upper()} {signal.action.upper()}.", "info")

    confirmation = technical.evaluate_signal(signal_payload, pipeline_state["research"])
    pipeline_state["technical_confirmation"] = confirmation

    if confirmation["status"] == "state_update":
        stage_status("tradingview", "waiting", confirmation["reason"])
        stage_status("decision", "idle", "Waiting for actionable BUY/SELL alert")
        pipeline_state["status"] = "watching"
        event(f"TradingView state update: {confirmation['reason']}", "info")
        return {
            "status": "state_update",
            "confirmation": confirmation,
            "decision": None,
            "execution": None
        }

    if confirmation["status"] != "confirmed":
        stage_status("tradingview", "blocked", confirmation["reason"])
        stage_status("decision", "skipped", "No options decision because confirmation failed")
        pipeline_state["status"] = "watching"
        event(f"Signal rejected: {confirmation['reason']}", "warning")
        return {
            "status": "skipped",
            "confirmation": confirmation,
            "decision": None,
            "execution": None
        }

    if is_real_tv:
        pipeline_state["tradingview"].update({
            "connected": True,
            "verified": True,
            "last_real_webhook_at": utc_now(),
            "secret_configured": bool(os.getenv("TRADINGVIEW_WEBHOOK_SECRET")),
        })
        stage_status("tradingview", "done", f"Verified TradingView alert for {signal.ticker.upper()}")
    else:
        stage_status("tradingview", "simulated", f"Test signal matched {signal.ticker.upper()}; real TradingView still not verified")
    stage_status("decision", "running", "Evaluating options risk and contract requirements")

    decision = trader.evaluate_trade(signal_payload, pipeline_state["research"], confirmation)
    pipeline_state["last_decision"] = decision

    if decision.get("decision") not in {"approved_for_paper_order", "test_plan_only"} or decision.get("confidence", 0) < 0.65:
        stage_status("decision", "skipped", decision.get("reason", "Final gate did not approve."))
        pipeline_state["status"] = "watching"
        event("Final options gate skipped the trade.", "warning")
        return {
            "status": "skipped",
            "confirmation": confirmation,
            "decision": decision,
            "execution": None
        }

    contract_symbol = decision.get("contract_plan", {}).get("symbol")
    stage_status(
        "decision",
        "done",
        f"Approved paper plan for {contract_symbol}" if is_real_tv else f"Test plan only for {contract_symbol}"
    )
    execution = {
        "status": "disabled",
        "message": "Alpaca execution is intentionally deferred to the next phase.",
        "symbol": contract_symbol,
    }
    pipeline_state["last_execution"] = execution
    stage_status("execution", "disabled", "Execution deferred; paper plan only")
    pipeline_state["status"] = "paper_approved" if is_real_tv else "test_approved"
    event(
        f"{'Decision approved for paper plan' if is_real_tv else 'Test plan generated'}: {contract_symbol}. Execution deferred.",
        "success"
    )
    return {
        "status": pipeline_state["status"],
        "confirmation": confirmation,
        "decision": decision,
        "execution": execution
    }


@app.on_event("startup")
def startup_scan() -> None:
    alpaca_option_stream.start()
    run_research_scan()


@app.on_event("shutdown")
def shutdown_stream() -> None:
    alpaca_option_stream.stop()


@app.get("/")
def serve_ui():
    return FileResponse("static/index.html")


@app.get("/api/pipeline")
def get_pipeline():
    refresh_alpaca_status()
    refresh_data_feed_status()
    pipeline_state["option_stream"] = alpaca_option_stream.snapshot()
    return pipeline_state


@app.post("/api/research/run")
async def research_run(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    digest = payload.get("market_digest") or DEFAULT_MARKET_DIGEST
    research = run_research_scan(digest)
    return {"status": "ok", "research": research, "pipeline": pipeline_state}


@app.get("/api/tradingview/watchlist")
def tradingview_watchlist():
    if not pipeline_state["research"]:
        run_research_scan()
    plans = [tradingview_indicator_plan(item) for item in pipeline_state["research"].get("watchlist", [])]
    return {
        "mode": "research_first",
        "watchlist": pipeline_state["research"].get("watchlist", []),
        "indicator_plans": plans,
        "alert_payload_template": {
            "secret": "{{your_TRADINGVIEW_WEBHOOK_SECRET}}",
            "ticker": "{{ticker}}",
            "action": "{{strategy.order.action}}",
            "price": "{{close}}",
            "time": "{{time}}",
            "interval": "{{interval}}",
            "indicator": "research_theme_options_v1",
            "option_symbol": ""
        },
        "webhook_requirements": {
            "public_url": "TradingView must call a public HTTPS URL on port 443 or HTTP on port 80.",
            "local_url": "/webhook/tradingview",
            "secret": "You create TRADINGVIEW_WEBHOOK_SECRET yourself and include the same value in .env and the alert JSON.",
            "timeout": "Keep webhook processing below 3 seconds; heavy research should already be cached before alerts arrive."
        }
    }


@app.get("/api/tradingview/setup")
def tradingview_setup():
    return tradingview_watchlist()


@app.get("/api/lean/watchlist")
def lean_watchlist():
    if not pipeline_state["research"]:
        run_research_scan()
    return {
        "status": "ok",
        "export": pipeline_state.get("lean", {}),
        "payload": build_lean_universe(pipeline_state["research"]),
    }


@app.get("/api/lean/status")
def lean_status():
    if not pipeline_state["research"]:
        run_research_scan()
    docker = lean_docker_status()
    pipeline_state["lean"]["docker"] = docker
    return {
        "status": "ok" if docker.get("image") == "installed" else "needs_setup",
        "lean": pipeline_state["lean"],
        "dry_check_command": "bash scripts/run_lean_dry_check.sh",
    }


@app.post("/api/lean/export")
def lean_export():
    if not pipeline_state["research"]:
        run_research_scan()
    output_path = write_lean_universe(pipeline_state["research"])
    payload = build_lean_universe(pipeline_state["research"])
    pipeline_state["lean"] = {
        "watchlist_export_path": str(output_path),
        "candidate_count": len(payload["candidates"]),
        "mode": payload["mode"],
        "generated_at": payload["generated_at"],
        "docker": lean_docker_status(),
    }
    event(f"LEAN watchlist export refreshed with {len(payload['candidates'])} candidates.", "info")
    return {"status": "ok", "export": pipeline_state["lean"], "payload": payload}


@app.get("/api/alpaca/status")
def alpaca_status():
    return {"status": "ok", "alpaca": refresh_alpaca_status(force=True)}


@app.get("/api/data-feeds/status")
def data_feed_status():
    if not pipeline_state["research"]:
        run_research_scan()
    feeds = refresh_data_feed_status(force=True)
    seed_option_stream_quotes(feeds)
    pipeline_state["option_stream"] = alpaca_option_stream.snapshot()
    return {"status": "ok", "data_feeds": feeds, "option_stream": pipeline_state["option_stream"]}


@app.get("/api/option-stream/status")
def option_stream_status():
    if not pipeline_state["research"]:
        run_research_scan()
    pipeline_state["option_stream"] = alpaca_option_stream.snapshot()
    return {"status": "ok", "option_stream": pipeline_state["option_stream"]}


@app.get("/api/sources/config")
def get_sources_config_route():
    from agents import get_custom_source_contracts, get_sources_config, get_sources_config_meta
    return {
        "status": "ok",
        "config": get_sources_config(),
        "meta": get_sources_config_meta(),
        "source_contracts": get_custom_source_contracts(),
    }


@app.post("/api/sources/config")
async def update_sources_config_route(request: Request):
    from agents import SourcesConfigValidationError, get_sources_config_meta, update_sources_config
    try:
        body = await request.json()
        updated = update_sources_config(body)
    except (json.JSONDecodeError, SourcesConfigValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    pipeline_state["data_feeds"]["checked_at"] = None
    event("Data sources configuration updated in real-time.", "info")
    return {"status": "ok", "config": updated, "meta": get_sources_config_meta()}


@app.post("/api/sources/config/reset")
async def reset_sources_config_route(request: Request):
    from agents import SourcesConfigValidationError, get_sources_config_meta, reset_sources_config
    try:
        body = await request.json()
        section = body.get("section") if isinstance(body, dict) else None
        updated = reset_sources_config(section)
    except (json.JSONDecodeError, SourcesConfigValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    pipeline_state["data_feeds"]["checked_at"] = None
    event(f"Data source configuration reset to defaults{f' for {section}' if section else ''}.", "info")
    return {"status": "ok", "config": updated, "meta": get_sources_config_meta()}


@app.put("/api/sources/config/source")
async def upsert_custom_source_route(request: Request):
    from agents import SourcesConfigValidationError, get_sources_config_meta, upsert_custom_source
    try:
        body = await request.json()
        section = body.get("section")
        source = body.get("source")
        updated = upsert_custom_source(section, source)
    except (json.JSONDecodeError, SourcesConfigValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    pipeline_state["data_feeds"]["checked_at"] = None
    event(f"Custom {section} source '{source.get('id')}' saved.", "info")
    return {"status": "ok", "config": updated, "meta": get_sources_config_meta()}


@app.post("/api/sources/config/source/test")
async def test_custom_source_route(request: Request):
    from agents import SourcesConfigValidationError, test_custom_source
    try:
        body = await request.json()
        result = test_custom_source(body.get("section"), body.get("source"), body.get("context"))
    except (json.JSONDecodeError, SourcesConfigValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return result


@app.delete("/api/sources/config/source/{section}/{source_id}")
def remove_custom_source_route(section: str, source_id: str):
    from agents import SourcesConfigValidationError, get_sources_config_meta, remove_custom_source
    try:
        updated = remove_custom_source(section, source_id)
    except (SourcesConfigValidationError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    pipeline_state["data_feeds"]["checked_at"] = None
    event(f"Custom {section} source '{source_id}' removed.", "info")
    return {"status": "ok", "config": updated, "meta": get_sources_config_meta()}


@app.post("/api/sources/config/calibrate/{section}")
def calibrate_sources_scoring_route(section: str):
    from agents import SourcesConfigValidationError, calibrate_source_scoring, get_sources_config_meta
    try:
        result = calibrate_source_scoring(section)
    except (SourcesConfigValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    pipeline_state["data_feeds"]["checked_at"] = None
    outcome = "applied" if result["applied"] else "rejected by holdout validation"
    event(f"{section.title()} score calibration {outcome}.", "info")
    return {"status": "ok", **result, "meta": get_sources_config_meta()}


@app.websocket("/ws/options")
async def option_stream_websocket(websocket: WebSocket):
    await websocket.accept()
    alpaca_option_stream.client_connected()
    last_sequence = None
    last_sent = 0.0
    try:
        while True:
            snapshot = alpaca_option_stream.snapshot()
            now = time.monotonic()
            if snapshot["sequence"] != last_sequence or now - last_sent >= 5:
                await websocket.send_json({"type": "option_stream", "stream": snapshot})
                last_sequence = snapshot["sequence"]
                last_sent = now
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass
    finally:
        alpaca_option_stream.client_disconnected()


@app.post("/api/simulate/signal")
def simulate_signal():
    if not pipeline_state.get("research", {}).get("watchlist"):
        run_research_scan()
    first = pipeline_state["research"]["watchlist"][0]
    action = "buy" if first["bias"] == "call" else "sell"
    signal = TradingViewSignal(
        action=action,
        ticker=first["ticker"],
        price=first.get("price"),
        time=utc_now(),
        interval="1D",
        indicator="local test signal",
        source="simulation",
        verified=False
    )
    return process_signal(signal)


@app.post("/webhook/tradingview")
async def receive_tradingview_webhook(webhook: Request):
    try:
        content_type = webhook.headers.get("content-type", "")
        if "application/json" in content_type:
            data = await webhook.json()
        else:
            body = (await webhook.body()).decode("utf-8")
            try:
                data = json.loads(body)
            except Exception:
                pairs = urllib.parse.parse_qs(body)
                data = {key: values[-1] for key, values in pairs.items()}
        verified = verify_tradingview_secret(data, webhook)
        pipeline_state["tradingview"].update({
            "connected": True,
            "verified": verified,
            "secret_configured": bool(os.getenv("TRADINGVIEW_WEBHOOK_SECRET")),
        })
        if not verified:
            event("Rejected TradingView webhook because the secret was missing or invalid.", "error")
            raise HTTPException(status_code=401, detail="TradingView webhook secret missing or invalid")
        signal = normalize_signal(data, source="tradingview", verified=verified)
        return process_signal(signal)
    except HTTPException:
        raise
    except Exception as exc:
        print(f"Error processing webhook: {exc}")
        raise HTTPException(status_code=400, detail=f"Invalid TradingView webhook payload: {exc}")


# ── Intelligence endpoints (US market intelligence via SEC EDGAR + Yahoo) ──

@app.get("/api/intelligence/{symbol}")
async def intelligence_all(symbol: str):
    """All 5 intelligence modules for a ticker in one call."""
    sym = symbol.upper().strip()
    try:
        return fetch_all_intelligence(sym)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/intelligence/{symbol}/insider")
async def intelligence_insider(symbol: str):
    """SEC EDGAR Form 4 insider trades for a ticker."""
    try:
        return fetch_insider_trades(symbol.upper())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/intelligence/{symbol}/congress")
async def intelligence_congress(symbol: str):
    """STOCK Act congressional trades for a ticker."""
    try:
        return fetch_congress_trades(symbol.upper())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/intelligence/{symbol}/actions")
async def intelligence_actions(symbol: str):
    """SEC EDGAR 8-K corporate actions + Yahoo dividends/splits."""
    try:
        return fetch_corporate_actions(symbol.upper())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/intelligence/{symbol}/ownership")
async def intelligence_ownership(symbol: str):
    """SEC EDGAR 13F institutional ownership for a ticker."""
    try:
        return fetch_ownership(symbol.upper())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/intelligence/{symbol}/seasonality")
async def intelligence_seasonality(symbol: str):
    """5-year monthly return seasonality from Yahoo Finance."""
    try:
        return fetch_seasonality(symbol.upper())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/intelligence/{symbol}/valuation")
async def intelligence_valuation(symbol: str):
    """Finviz analyst price targets, recommendations, short float %, and valuation multiples."""
    try:
        return fetch_analyst_and_valuation(symbol.upper())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/health")
def health_check():
    alpaca = refresh_alpaca_status()
    feeds = refresh_data_feed_status()
    stream = alpaca_option_stream.snapshot()
    return {
        "status": "ok",
        "mode": pipeline_state["mode"],
        "alpaca": alpaca,
        "lean": pipeline_state.get("lean", {}),
        "data_feeds": {
            "status": feeds.get("status"),
            "detail": feeds.get("detail"),
            "provider_counts": feeds.get("provider_counts"),
        },
        "option_stream": {
            "status": stream.get("status"),
            "detail": stream.get("detail"),
            "feed": stream.get("feed"),
            "subscribed_count": stream.get("subscribed_count"),
            "quote_count": stream.get("quote_count"),
            "stream_quote_count": stream.get("stream_quote_count"),
        },
        "alpaca_trading_enabled": os.getenv("ALPACA_TRADING_ENABLED", "false").lower() == "true",
        "tradingview_secret_configured": bool(os.getenv("TRADINGVIEW_WEBHOOK_SECRET")),
        "execution_phase": "disabled"
    }


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
