"""
Anthropic Claude integration for the trading pipeline.
Provides tiered model routing (Sonnet for bulk, Opus for reasoning),
response caching, cost tracking, and structured JSON output.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    import anthropic
except ImportError:
    anthropic = None

MODEL_ALIASES = {
    "sonnet": "claude-sonnet-4-20250514",
    "opus": "claude-opus-4-20250514",
    "disabled": None,
}

# Approximate USD per 1M tokens (input / output)
MODEL_PRICING = {
    "claude-sonnet-4-20250514": (3.0, 15.0),
    "claude-opus-4-20250514": (15.0, 75.0),
}

_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_LOCK = threading.RLock()
_COST_LOCK = threading.RLock()
_SESSION_COST = {"input_tokens": 0, "output_tokens": 0, "calls": 0, "estimated_usd": 0.0}
_ACTIVITY_LOCK = threading.RLock()
_ACTIVITY_LOG: list = []


def log_llm_activity(stage: str, status: str, model: str = "", summary: str = "", detail: Any = None, error: str = None) -> None:
    entry = {
        "time": utc_now(),
        "stage": stage,
        "status": status,
        "model": model,
        "summary": summary,
        "detail": detail,
        "error": error,
    }
    with _ACTIVITY_LOCK:
        _ACTIVITY_LOG.insert(0, entry)
        del _ACTIVITY_LOG[50:]


def get_llm_activity() -> list:
    with _ACTIVITY_LOCK:
        return list(_ACTIVITY_LOG)


def get_llm_status() -> Dict[str, Any]:
    cfg = llm_config()
    api_key_set = bool(os.getenv("ANTHROPIC_API_KEY", "").strip())
    sdk_installed = anthropic is not None
    stage_ids = ("discovery", "news", "social", "intelligence", "debate", "options", "decision", "exit", "portfolio")
    stages = {
        stage: {
            "enabled": llm_enabled(stage) if stage != "portfolio" else llm_enabled("exit"),
            "toggle": f"use_llm_{stage}" if stage != "portfolio" else "use_llm_exit",
        }
        for stage in stage_ids
    }
    master = bool(cfg.get("enabled", True))
    ready = api_key_set and sdk_installed and master
    if not api_key_set:
        reason = "Set ANTHROPIC_API_KEY in .env"
    elif not sdk_installed:
        reason = "Run: pip install anthropic"
    elif not master:
        reason = "LLM master switch is off (Config → LLM)"
    else:
        reason = "Ready — runs on next scan / position check"
    return {
        "configured": ready,
        "api_key_set": api_key_set,
        "sdk_installed": sdk_installed,
        "master_enabled": master,
        "sonnet_model": cfg.get("sonnet_model") or MODEL_ALIASES["sonnet"],
        "opus_model": cfg.get("opus_model") or MODEL_ALIASES["opus"],
        "stages": stages,
        "reason": reason,
        "session_cost": get_session_cost(),
    }


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def reset_session_cost() -> None:
    with _COST_LOCK:
        _SESSION_COST.update({"input_tokens": 0, "output_tokens": 0, "calls": 0, "estimated_usd": 0.0})


def get_session_cost() -> Dict[str, Any]:
    with _COST_LOCK:
        return dict(_SESSION_COST)


def _estimate_cost(model_id: str, input_tokens: int, output_tokens: int) -> float:
    pricing = MODEL_PRICING.get(model_id, (3.0, 15.0))
    return (input_tokens * pricing[0] + output_tokens * pricing[1]) / 1_000_000


def _cache_key(prompt: str, model: str, system: str = "") -> str:
    digest = hashlib.sha256(f"{model}|{system}|{prompt}".encode()).hexdigest()
    return digest


def _get_cached(key: str, ttl_seconds: int) -> Optional[str]:
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if not entry:
            return None
        if time.time() - entry["ts"] > ttl_seconds:
            del _CACHE[key]
            return None
        return entry["response"]


def _set_cache(key: str, response: str) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = {"ts": time.time(), "response": response}
        if len(_CACHE) > 200:
            oldest = sorted(_CACHE.items(), key=lambda x: x[1]["ts"])[:50]
            for k, _ in oldest:
                _CACHE.pop(k, None)


def _extract_json(text: str) -> Any:
    text = (text or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            pass
    start = text.find("[")
    end = text.rfind("]")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            pass
    return None


def llm_config() -> Dict[str, Any]:
    try:
        from agents import get_sources_config
        return get_sources_config().get("llm", {})
    except Exception:
        return {}


def llm_enabled(stage: str) -> bool:
    cfg = llm_config()
    if not cfg.get("enabled", True):
        return False
    flag = cfg.get(f"use_llm_{stage}", True)
    return bool(flag)


def resolve_model(alias: str) -> Optional[str]:
    cfg = llm_config()
    if alias == "sonnet":
        return cfg.get("sonnet_model") or MODEL_ALIASES["sonnet"]
    if alias == "opus":
        return cfg.get("opus_model") or MODEL_ALIASES["opus"]
    return MODEL_ALIASES.get(alias)


class OmniRouteClient:
  """Anthropic Claude client with caching, retries, and cost tracking."""

  def __init__(self, api_key: str = None):
    self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "")
    self._client = None

  def _get_client(self):
    if self._client is not None:
      return self._client
    if anthropic is None:
      raise RuntimeError("anthropic package not installed. Run: pip install anthropic")
    if not self.api_key:
      raise RuntimeError("ANTHROPIC_API_KEY is not configured")
    self._client = anthropic.Anthropic(api_key=self.api_key)
    return self._client

  def generate_response(
    self,
    prompt: str,
    model: str = "sonnet",
    system: str = "",
    max_tokens: int = 2048,
    temperature: float = 0.2,
    use_cache: bool = True,
  ) -> str:
    model_id = resolve_model(model)
    if not model_id:
      return json.dumps({"mode": "disabled", "generated_at": utc_now()})

    cfg = llm_config()
    cache_ttl = int(cfg.get("cache_ttl_seconds", 900))
    cache_key = _cache_key(prompt, model_id, system)
    if use_cache:
      cached = _get_cached(cache_key, cache_ttl)
      if cached is not None:
        log_llm_activity("cache", "cached", model_id, f"Reused cached response ({len(prompt)} chars)")
        return cached

    client = self._get_client()
    retries = int(cfg.get("max_retries", 2))
    last_error = None
    for attempt in range(retries + 1):
      try:
        kwargs = {
          "model": model_id,
          "max_tokens": max_tokens,
          "temperature": temperature,
          "messages": [{"role": "user", "content": prompt}],
        }
        if system:
          kwargs["system"] = system
        response = client.messages.create(**kwargs)
        text = "".join(block.text for block in response.content if hasattr(block, "text"))
        input_tokens = getattr(response.usage, "input_tokens", 0) or 0
        output_tokens = getattr(response.usage, "output_tokens", 0) or 0
        with _COST_LOCK:
          _SESSION_COST["input_tokens"] += input_tokens
          _SESSION_COST["output_tokens"] += output_tokens
          _SESSION_COST["calls"] += 1
          _SESSION_COST["estimated_usd"] = round(
            _SESSION_COST["estimated_usd"] + _estimate_cost(model_id, input_tokens, output_tokens),
            6,
          )
        if use_cache:
          _set_cache(cache_key, text)
        return text
      except Exception as exc:
        last_error = exc
        if attempt < retries:
          time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"LLM call failed after {retries + 1} attempts: {last_error}")

  def generate_structured(
    self,
    prompt: str,
    model: str = "sonnet",
    system: str = "",
    max_tokens: int = 2048,
    temperature: float = 0.1,
    use_cache: bool = True,
  ) -> Dict[str, Any]:
    system_prompt = (system or "") + "\nRespond with valid JSON only. No markdown."
    raw = self.generate_response(
      prompt,
      model=model,
      system=system_prompt.strip(),
      max_tokens=max_tokens,
      temperature=temperature,
      use_cache=use_cache,
    )
    parsed = _extract_json(raw)
    if parsed is None:
      return {"error": "failed_to_parse_json", "raw": raw[:500]}
    return parsed if isinstance(parsed, dict) else {"data": parsed}


# ── Stage-specific LLM helpers ─────────────────────────────────────────────

def llm_news_sentiment(theme_name: str, headlines: list, client: OmniRouteClient) -> Dict[str, Any]:
  if not llm_enabled("news"):
    log_llm_activity("news", "skipped", "", f"News LLM off for {theme_name}")
    return {}
  if not headlines:
    return {}
  titles = [h.get("title", "") for h in headlines[:12] if h.get("title")]
  if not titles:
    return {}
  log_llm_activity("news", "running", "sonnet", f"Analyzing {len(titles)} headlines for {theme_name}")
  prompt = (
    f"Analyze news sentiment for the '{theme_name}' market theme.\n"
    f"Headlines:\n" + "\n".join(f"- {t}" for t in titles) + "\n\n"
    "Return JSON: {\"sentiment_score\": 0-100, \"catalysts\": [\"...\"], \"risk_flags\": [\"...\"], \"summary\": \"...\"}"
  )
  try:
    result = client.generate_structured(prompt, model="sonnet", max_tokens=800)
    if result.get("error"):
      log_llm_activity("news", "error", "sonnet", theme_name, error=str(result.get("error"))[:200])
    else:
      log_llm_activity("news", "done", "sonnet", f"{theme_name}: score {result.get('sentiment_score', '?')}", result.get("summary"))
    return result
  except Exception as exc:
    log_llm_activity("news", "error", "sonnet", theme_name, error=str(exc)[:200])
    return {"error": str(exc)[:200]}


def llm_social_sentiment(theme_name: str, posts: list, client: OmniRouteClient) -> Dict[str, Any]:
  if not posts or not llm_enabled("social"):
    return {}
  titles = [p.get("title", "") for p in posts[:10] if p.get("title")]
  if not titles:
    return {}
  prompt = (
    f"Analyze retail social sentiment for '{theme_name}'.\n"
    f"Posts:\n" + "\n".join(f"- {t}" for t in titles) + "\n\n"
    "Return JSON: {\"sentiment_score\": 0-100, \"momentum\": \"rising|falling|stable\", "
    "\"meme_risk\": true/false, \"summary\": \"...\"}"
  )
  try:
    return client.generate_structured(prompt, model="sonnet", max_tokens=800)
  except Exception as exc:
    return {"error": str(exc)[:200]}


def llm_discovery_themes(headlines: list, existing_themes: list, client: OmniRouteClient) -> Dict[str, Any]:
  if not headlines or not llm_enabled("discovery"):
    return {}
  titles = headlines[:20]
  existing = ", ".join(existing_themes[:8])
  prompt = (
    "You are a market analyst. From these headlines, suggest 2-4 emerging trading themes "
    "not already covered.\n\n"
    f"Existing themes: {existing}\n\n"
    "Headlines:\n" + "\n".join(f"- {t}" for t in titles) + "\n\n"
    "Return JSON: {\"themes\": [{\"name\": \"...\", \"tickers\": [\"SYM\"], \"reasoning\": \"...\"}]}"
  )
  try:
    return client.generate_structured(prompt, model="sonnet", max_tokens=1200)
  except Exception as exc:
    return {"error": str(exc)[:200]}


def llm_intelligence_synthesis(symbol: str, intel_data: dict, client: OmniRouteClient) -> Dict[str, Any]:
  if not llm_enabled("intelligence"):
    return {}
  compact = {
    "insider": intel_data.get("insider", {}),
    "congress": intel_data.get("congress", {}),
    "valuation": {
      k: intel_data.get("valuation", {}).get(k)
      for k in ("target_upside_pct", "recommendation_label", "earnings_days_away", "earnings_date", "short_float_num")
    },
    "seasonality": intel_data.get("seasonality", {}).get("current_month"),
    "corporate_actions": intel_data.get("corporate_actions", {}).get("summary"),
  }
  prompt = (
    f"Synthesize intelligence signals for {symbol}:\n{json.dumps(compact, default=str)[:3000]}\n\n"
    "Return JSON: {\"intelligence_score\": 0-100, \"composite_signal\": \"bullish|bearish|neutral\", "
    "\"bull_catalysts\": [\"...\"], \"bear_catalysts\": [\"...\"], \"catalyst_timeline\": \"...\", "
    "\"risk_assessment\": \"...\"}"
  )
  try:
    return client.generate_structured(prompt, model="sonnet", max_tokens=1000)
  except Exception as exc:
    return {"error": str(exc)[:200]}


def llm_debate(context: dict, client: OmniRouteClient) -> Dict[str, Any]:
  if not llm_enabled("debate"):
    log_llm_activity("debate", "skipped", "", "Debate LLM disabled")
    return {}
  log_llm_activity("debate", "running", "opus", "Bull/bear/risk adversarial debate")
  prompt = (
    "You are running a trading research debate. Analyze the data and produce bull, bear, and risk manager views.\n\n"
    f"Context:\n{json.dumps(context, default=str)[:6000]}\n\n"
    "Return JSON: {\"bull\": [\"point1\", ...], \"bear\": [\"point1\", ...], "
    "\"risk\": {\"market_regime\": \"risk_on|caution|risk_off\", \"notes\": \"...\"}, "
    "\"manager\": \"final recommendation\"}"
  )
  try:
    result = client.generate_structured(prompt, model="opus", max_tokens=2000, temperature=0.3)
    if result.get("error"):
      log_llm_activity("debate", "error", "opus", "Debate failed", error=str(result.get("error"))[:200])
    else:
      log_llm_activity("debate", "done", "opus", result.get("manager", "Debate complete")[:120])
    return result
  except Exception as exc:
    log_llm_activity("debate", "error", "opus", "Debate failed", error=str(exc)[:200])
    return {"error": str(exc)[:200]}


def llm_options_context(symbol: str, contracts: list, catalysts: list, client: OmniRouteClient) -> Dict[str, Any]:
  if not contracts or not llm_enabled("options"):
    return {}
  prompt = (
    f"Evaluate option contracts for {symbol}. Consider earnings/catalyst timing.\n"
    f"Catalysts: {catalysts}\n"
    f"Contracts: {json.dumps(contracts[:3], default=str)[:2000]}\n\n"
    "Return JSON: {\"recommended_symbol\": \"OCC symbol or null\", \"reasoning\": \"...\", "
    "\"earnings_warning\": true/false, \"adjustment\": \"none|avoid|prefer_shorter_dte\"}"
  )
  try:
    return client.generate_structured(prompt, model="opus", max_tokens=1000)
  except Exception as exc:
    return {"error": str(exc)[:200]}


def llm_trade_decision(context: dict, client: OmniRouteClient) -> Dict[str, Any]:
  if not llm_enabled("decision"):
    log_llm_activity("decision", "skipped", "", "Trade decision LLM disabled")
    return {}
  ticker = (context.get("ticker") or context.get("watch", {}).get("ticker") or "?")
  log_llm_activity("decision", "running", "opus", f"Final gate for {ticker}")
  prompt = (
    "You are the final trade decision gate for an options trading system. "
    "Risk checks have already passed as pre-filters. Make a nuanced go/no-go decision.\n\n"
    f"Context:\n{json.dumps(context, default=str)[:6000]}\n\n"
    "Return JSON: {\"decision\": \"approve|skip|reduce_size\", \"confidence\": 0.0-1.0, "
    "\"reasoning\": \"...\", \"size_multiplier\": 0.5-1.0}"
  )
  try:
    result = client.generate_structured(prompt, model="opus", max_tokens=1200, temperature=0.2)
    if result.get("error"):
      log_llm_activity("decision", "error", "opus", ticker, error=str(result.get("error"))[:200])
    else:
      log_llm_activity("decision", "done", "opus", f"{ticker}: {result.get('decision', '?')} — {str(result.get('reasoning', ''))[:80]}")
    return result
  except Exception as exc:
    log_llm_activity("decision", "error", "opus", ticker, error=str(exc)[:200])
    return {"error": str(exc)[:200]}


def llm_exit_reasoning(position: dict, market_context: dict, client: OmniRouteClient) -> Dict[str, Any]:
  if not llm_enabled("exit"):
    return {}
  symbol = position.get("symbol", "?")
  log_llm_activity("exit", "running", "opus", f"Exit review: {symbol}")
  prompt = (
    "Evaluate whether to hold or exit this options position. "
    "Config thresholds are guidelines — use live market context. "
    "If thesis still valid and theta manageable, prefer hold.\n\n"
    f"Position: {json.dumps(position, default=str)[:2500]}\n"
    f"Market: {json.dumps(market_context, default=str)[:1500]}\n\n"
    "Return JSON: {\"action\": \"hold|exit\", \"urgency\": 0-100, \"reasoning\": \"...\", "
    "\"thesis_valid\": true/false, \"override_hard_limit\": true/false}"
  )
  try:
    result = client.generate_structured(prompt, model="opus", max_tokens=800, temperature=0.2)
    if result.get("error"):
      log_llm_activity("exit", "error", "opus", symbol, error=str(result.get("error"))[:200])
    else:
      log_llm_activity("exit", "done", "opus", f"{symbol}: {result.get('action', '?')} — {str(result.get('reasoning', ''))[:80]}")
    return result
  except Exception as exc:
    log_llm_activity("exit", "error", "opus", symbol, error=str(exc)[:200])
    return {"error": str(exc)[:200]}


def llm_portfolio_review(positions: list, market_context: dict, client: OmniRouteClient) -> Dict[str, Any]:
  if not llm_enabled("exit") or not positions:
    return {}
  log_llm_activity("portfolio", "running", "opus", f"Reviewing {len(positions)} open position(s)")
  prompt = (
    "Review the options portfolio holistically. Assess P&L, theta risk, hold duration, and market regime. "
    "Recommend per-position hold/exit and overall portfolio health.\n\n"
    f"Positions: {json.dumps(positions[:8], default=str)[:5000]}\n"
    f"Market: {json.dumps(market_context, default=str)[:2000]}\n\n"
    "Return JSON: {\"portfolio_health\": \"good|mixed|poor\", \"health_score\": 0-100, "
    "\"summary\": \"...\", \"positions\": [{\"symbol\": \"...\", \"action\": \"hold|exit\", \"reasoning\": \"...\"}], "
    "\"recommend_rescan\": true/false}"
  )
  try:
    result = client.generate_structured(prompt, model="opus", max_tokens=1500, temperature=0.2)
    if result.get("error"):
      log_llm_activity("portfolio", "error", "opus", "Portfolio review failed", error=str(result.get("error"))[:200])
    else:
      log_llm_activity("portfolio", "done", "opus", result.get("summary", "Portfolio review complete")[:120])
    return result
  except Exception as exc:
    log_llm_activity("portfolio", "error", "opus", "Portfolio review failed", error=str(exc)[:200])
    return {"error": str(exc)[:200]}
