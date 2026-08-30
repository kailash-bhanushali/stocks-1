from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import os
import re
import time
import urllib.parse

import requests
from dotenv import load_dotenv

from source_adapters import GenericJsonSource

try:
    from alpaca.data.enums import DataFeed, OptionsFeed
    from alpaca.data.historical import OptionHistoricalDataClient, StockHistoricalDataClient
    from alpaca.data.requests import OptionSnapshotRequest, StockSnapshotRequest
except Exception:
    DataFeed = OptionsFeed = None
    OptionHistoricalDataClient = StockHistoricalDataClient = None
    OptionSnapshotRequest = StockSnapshotRequest = None


HTTP_HEADERS = {
    "User-Agent": os.getenv("RESEARCH_USER_AGENT", "trading-research-bot/0.1 contact:kailash-local")
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def safe_round(value, digits=4):
    value = safe_float(value)
    if value is None:
        return None
    return round(value, digits)


def sanitize_error(value, limit=240):
    text = str(value)
    text = re.sub(r"([?&](?:apiKey|apikey|token|key|secret)=)[^&\s]+", r"\1***", text)
    for name in [
        "ALPACA_API_KEY",
        "ALPACA_SECRET_KEY",
        "FINNHUB_API_KEY",
        "ALPHA_VANTAGE_API_KEY",
        "MASSIVE_API_KEY",
        "POLYGON_API_KEY",
        "TRADINGVIEW_WEBHOOK_SECRET",
    ]:
        secret = os.getenv(name, "")
        if secret:
            text = text.replace(secret, "***")
    return text[:limit]


def timed_provider(provider_id, label, fn):
    started = time.perf_counter()
    try:
        payload = fn()
        status = payload.get("status", "ok")
        detail = payload.get("detail", "ok")
    except Exception as exc:
        payload = {"status": "error", "detail": sanitize_error(exc)}
        status = "error"
        detail = payload["detail"]
    payload.update({
        "provider": provider_id,
        "label": label,
        "latency_ms": int((time.perf_counter() - started) * 1000),
        "status": status,
        "detail": detail,
    })
    return payload


class DataFeedRouter:
    def __init__(self):
        self._load_config()

    def _load_config(self):
        load_dotenv(override=True)
        self.alpaca_key = os.getenv("ALPACA_API_KEY", "")
        self.alpaca_secret = os.getenv("ALPACA_SECRET_KEY", "")
        self.finnhub_key = os.getenv("FINNHUB_API_KEY", "")
        self.alpha_vantage_key = os.getenv("ALPHA_VANTAGE_API_KEY", "")
        self.massive_key = os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY", "")
        self.stock_feed = os.getenv("ALPACA_STOCK_FEED", "iex").lower()
        self.option_feed = os.getenv("ALPACA_OPTION_FEED", "indicative").lower()

    def snapshot(self, watchlist, config=None):
        self._load_config()
        config = config or {}
        enabled_sources = set(config.get("enabled_sources", ["alpaca", "cboe", "finnhub", "alpha_vantage", "massive"]))
        symbols = [
            item["ticker"] for item in watchlist if item.get("ticker")
        ][:int(config.get("max_quote_symbols", 8))]
        contracts = [
            (item.get("options") or {}).get("selected_contract", {}).get("symbol")
            for item in watchlist
        ]
        contracts = [contract for contract in contracts if contract][:int(config.get("max_quote_contracts", 8))]

        providers = []
        provider_jobs = {
            "alpaca": ("Alpaca market data", lambda: self._alpaca_snapshots(symbols, contracts)),
            "cboe": ("Cboe delayed options", lambda: self._cboe_option_quotes(contracts, config)),
            "finnhub": ("Finnhub stock quotes", lambda: self._finnhub_quotes(symbols[:4], config)),
            "alpha_vantage": ("Alpha Vantage quote", lambda: self._alpha_vantage_quote(symbols[:1], config)),
            "massive": ("Massive stock snapshots", lambda: self._massive_snapshots(symbols[:4], config)),
        }
        for source in config.get("custom_sources", []):
            if source.get("enabled") and source.get("adapter") == "quote_json":
                provider_jobs[source["id"]] = (
                    source["label"],
                    lambda source=source: self._custom_quotes(symbols, source),
                )
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [
                executor.submit(timed_provider, provider_id, provider_jobs[provider_id][0], provider_jobs[provider_id][1])
                for provider_id in provider_jobs
                if provider_id in enabled_sources or provider_id in {
                    source.get("id") for source in config.get("custom_sources", []) if source.get("enabled")
                }
            ]
            for future in as_completed(futures):
                providers.append(future.result())

        underlyings = self._merge_underlyings(symbols, providers, config)
        options = self._merge_options(contracts, providers)
        return {
            "generated_at": utc_now(),
            "mode": "read_only_router",
            "symbols": symbols,
            "contracts": contracts,
            "enabled_sources": sorted(enabled_sources),
            "primary": {
                "underlying": self._first_ok_provider(providers, "underlyings", config),
                "options": self._first_ok_provider(providers, "options"),
            },
            "providers": sorted(providers, key=lambda item: item["provider"]),
            "underlyings": underlyings,
            "options": options,
            "notes": [
                "Router is read-only; order execution is still disabled.",
                "Alpaca option feed defaults to indicative unless OPRA is configured.",
                "TradingView remains optional; indicators can run in code/LEAN.",
            ],
        }

    def _alpaca_snapshots(self, symbols, contracts):
        if not self.alpaca_key or not self.alpaca_secret:
            return {"status": "not_configured", "detail": "ALPACA_API_KEY/ALPACA_SECRET_KEY missing", "underlyings": {}, "options": {}}
        if not StockHistoricalDataClient or not OptionHistoricalDataClient:
            return {"status": "unavailable", "detail": "alpaca-py market data SDK unavailable", "underlyings": {}, "options": {}}

        underlyings = {}
        options = {}
        errors = {}
        stock_feed = self._alpaca_stock_feed()
        option_feed = self._alpaca_option_feed()
        if symbols:
            try:
                stock_client = StockHistoricalDataClient(self.alpaca_key, self.alpaca_secret, raw_data=True)
                raw = stock_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=symbols, feed=stock_feed))
                for symbol, snapshot in (raw or {}).items():
                    quote = snapshot.get("latestQuote") or snapshot.get("latest_quote") or {}
                    trade = snapshot.get("latestTrade") or snapshot.get("latest_trade") or {}
                    minute = snapshot.get("minuteBar") or snapshot.get("minute_bar") or {}
                    price = safe_float(trade.get("p") or trade.get("price") or minute.get("c"))
                    bid = safe_float(quote.get("bp") or quote.get("bid_price"))
                    ask = safe_float(quote.get("ap") or quote.get("ask_price"))
                    underlyings[symbol] = {
                        "symbol": symbol,
                        "provider": "alpaca",
                        "feed": self.stock_feed,
                        "price": safe_round(price),
                        "bid": safe_round(bid),
                        "ask": safe_round(ask),
                        "mid": safe_round((bid + ask) / 2) if bid and ask else safe_round(price),
                        "timestamp": trade.get("t") or quote.get("t") or minute.get("t"),
                    }
            except Exception as exc:
                errors["stocks"] = sanitize_error(exc, 220)

        if contracts:
            try:
                option_client = OptionHistoricalDataClient(self.alpaca_key, self.alpaca_secret, raw_data=True)
                raw = option_client.get_option_snapshot(OptionSnapshotRequest(symbol_or_symbols=contracts, feed=option_feed))
                for symbol, snapshot in (raw or {}).items():
                    quote = snapshot.get("latestQuote") or snapshot.get("latest_quote") or {}
                    trade = snapshot.get("latestTrade") or snapshot.get("latest_trade") or {}
                    greeks = snapshot.get("greeks") or {}
                    bid = safe_float(quote.get("bp") or quote.get("bid_price"))
                    ask = safe_float(quote.get("ap") or quote.get("ask_price"))
                    price = safe_float(trade.get("p") or trade.get("price"))
                    mid = (bid + ask) / 2 if bid and ask else price
                    options[symbol] = {
                        "symbol": symbol,
                        "provider": "alpaca",
                        "feed": self.option_feed,
                        "bid": safe_round(bid),
                        "ask": safe_round(ask),
                        "mid": safe_round(mid),
                        "price": safe_round(price),
                        "spread_pct": self._spread_pct(bid, ask, mid),
                        "iv": safe_round(snapshot.get("impliedVolatility") or snapshot.get("implied_volatility")),
                        "delta": safe_round(greeks.get("delta")),
                        "theta": safe_round(greeks.get("theta")),
                        "vega": safe_round(greeks.get("vega")),
                        "timestamp": trade.get("t") or quote.get("t"),
                    }
            except Exception as exc:
                errors["options"] = sanitize_error(exc, 220)

        status = "ok" if underlyings or options else "error" if errors else "empty"
        return {
            "status": status,
            "detail": f"{len(underlyings)} stock snapshots, {len(options)} option snapshots",
            "underlyings": underlyings,
            "options": options,
            "errors": errors,
        }

    def _finnhub_quotes(self, symbols, config):
        if not self.finnhub_key:
            return {"status": "not_configured", "detail": "FINNHUB_API_KEY missing", "underlyings": {}}
        quotes = {}
        errors = {}
        for symbol in symbols:
            try:
                response = requests.get(
                    config.get("finnhub_quote_endpoint", "https://finnhub.io/api/v1/quote"),
                    params={"symbol": symbol, "token": self.finnhub_key},
                    timeout=float(config.get("request_timeout_seconds", 8)),
                )
                response.raise_for_status()
                data = response.json()
                price = safe_float(data.get("c"))
                if price:
                    quotes[symbol] = {
                        "symbol": symbol,
                        "provider": "finnhub",
                        "price": safe_round(price),
                        "previous_close": safe_round(data.get("pc")),
                        "open": safe_round(data.get("o")),
                        "high": safe_round(data.get("h")),
                        "low": safe_round(data.get("l")),
                        "timestamp": data.get("t"),
                    }
            except Exception as exc:
                errors[symbol] = sanitize_error(exc, 160)
        return {
            "status": "ok" if quotes else "error" if errors else "empty",
            "detail": f"{len(quotes)}/{len(symbols)} quotes",
            "underlyings": quotes,
            "errors": errors,
        }

    def _alpha_vantage_quote(self, symbols, config):
        if not self.alpha_vantage_key:
            return {"status": "not_configured", "detail": "ALPHA_VANTAGE_API_KEY missing", "underlyings": {}}
        quotes = {}
        errors = {}
        for symbol in symbols:
            try:
                response = requests.get(
                    config.get("alpha_vantage_endpoint", "https://www.alphavantage.co/query"),
                    params={"function": "GLOBAL_QUOTE", "symbol": symbol, "apikey": self.alpha_vantage_key},
                    timeout=float(config.get("request_timeout_seconds", 8)),
                )
                response.raise_for_status()
                data = response.json()
                quote = data.get("Global Quote") or {}
                price = safe_float(quote.get("05. price"))
                if price:
                    quotes[symbol] = {
                        "symbol": symbol,
                        "provider": "alpha_vantage",
                        "price": safe_round(price),
                        "previous_close": safe_round(quote.get("08. previous close")),
                        "change_pct": quote.get("10. change percent"),
                        "volume": safe_float(quote.get("06. volume")),
                        "timestamp": quote.get("07. latest trading day"),
                    }
                else:
                    errors[symbol] = str(data.get("Information") or data.get("Note") or "No Global Quote")[:160]
            except Exception as exc:
                errors[symbol] = sanitize_error(exc, 160)
        return {
            "status": "ok" if quotes else "error" if errors else "empty",
            "detail": f"{len(quotes)}/{len(symbols)} quotes",
            "underlyings": quotes,
            "errors": errors,
        }

    def _massive_snapshots(self, symbols, config):
        if not self.massive_key:
            return {"status": "not_configured", "detail": "MASSIVE_API_KEY missing", "underlyings": {}}
        snapshots = {}
        errors = {}
        for symbol in symbols:
            try:
                response = requests.get(
                    config.get(
                        "massive_endpoint",
                        "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}",
                    ).replace("{symbol}", urllib.parse.quote(symbol)),
                    params={"apiKey": self.massive_key},
                    timeout=float(config.get("request_timeout_seconds", 8)),
                )
                response.raise_for_status()
                data = response.json()
                ticker = data.get("ticker") or {}
                last_trade = ticker.get("lastTrade") or {}
                last_quote = ticker.get("lastQuote") or {}
                day = ticker.get("day") or {}
                price = safe_float(last_trade.get("p") or day.get("c"))
                bid = safe_float(last_quote.get("p"))
                ask = safe_float(last_quote.get("P"))
                if price or bid or ask:
                    snapshots[symbol] = {
                        "symbol": symbol,
                        "provider": "massive",
                        "price": safe_round(price),
                        "bid": safe_round(bid),
                        "ask": safe_round(ask),
                        "mid": safe_round((bid + ask) / 2) if bid and ask else safe_round(price),
                        "change_pct": safe_round(ticker.get("todaysChangePerc")),
                        "volume": safe_float(day.get("v")),
                        "timestamp": ticker.get("updated") or last_trade.get("t"),
                    }
            except Exception as exc:
                errors[symbol] = sanitize_error(exc, 160)
        return {
            "status": "ok" if snapshots else "error" if errors else "empty",
            "detail": f"{len(snapshots)}/{len(symbols)} snapshots",
            "underlyings": snapshots,
            "errors": errors,
        }

    def _custom_quotes(self, symbols, source):
        quotes = {}
        errors = {}
        adapter = GenericJsonSource(source)
        for symbol in symbols:
            try:
                quote = adapter.fetch_quote(symbol)
                bid = safe_float(quote.get("bid"))
                ask = safe_float(quote.get("ask"))
                price = safe_float(quote.get("price"))
                quote.update({
                    "price": safe_round(price),
                    "bid": safe_round(bid),
                    "ask": safe_round(ask),
                    "mid": safe_round((bid + ask) / 2) if bid and ask else safe_round(price),
                })
                quotes[symbol] = quote
            except Exception as exc:
                errors[symbol] = sanitize_error(exc, 160)
        return {
            "status": "ok" if quotes else "error" if errors else "empty",
            "detail": f"{len(quotes)}/{len(symbols)} custom quotes",
            "underlyings": quotes,
            "errors": errors,
        }

    def _cboe_option_quotes(self, contracts, config):
        if not contracts:
            return {"status": "empty", "detail": "No contracts selected", "options": {}}
        by_underlying = {}
        for contract in contracts:
            underlying = self._occ_underlying(contract)
            if underlying:
                by_underlying.setdefault(underlying, []).append(contract)
        options = {}
        errors = {}
        for underlying, target_contracts in by_underlying.items():
            try:
                response = requests.get(
                    config.get(
                        "cboe_endpoint",
                        "https://cdn.cboe.com/api/global/delayed_quotes/options/{underlying}.json",
                    ).replace("{underlying}", urllib.parse.quote(underlying)),
                    headers=HTTP_HEADERS,
                    timeout=float(config.get("request_timeout_seconds", 12)),
                )
                response.raise_for_status()
                payload = response.json()
                rows = payload.get("data", {}).get("options") or []
                by_contract = {row.get("option"): row for row in rows}
                for contract in target_contracts:
                    row = by_contract.get(contract)
                    if not row:
                        continue
                    bid = safe_float(row.get("bid"))
                    ask = safe_float(row.get("ask"))
                    last = safe_float(row.get("last_trade_price"))
                    mid = (bid + ask) / 2 if bid and ask else last
                    options[contract] = {
                        "symbol": contract,
                        "provider": "cboe",
                        "feed": "delayed",
                        "bid": safe_round(bid),
                        "ask": safe_round(ask),
                        "mid": safe_round(mid),
                        "price": safe_round(last),
                        "spread_pct": self._spread_pct(bid, ask, mid),
                        "iv": safe_round(row.get("iv")),
                        "delta": safe_round(row.get("delta")),
                        "open_interest": int(safe_float(row.get("open_interest")) or 0),
                        "volume": int(safe_float(row.get("volume")) or 0),
                        "timestamp": payload.get("timestamp"),
                    }
            except Exception as exc:
                errors[underlying] = sanitize_error(exc, 160)
        return {
            "status": "ok" if options else "error" if errors else "empty",
            "detail": f"{len(options)}/{len(contracts)} delayed option quotes",
            "options": options,
            "errors": errors,
        }

    def _provider_priorities(self, config):
        priorities = {
            "alpaca": 100,
            "cboe": 100,
            "finnhub": 200,
            "massive": 300,
            "alpha_vantage": 400,
        }
        for source in config.get("custom_sources", []):
            priorities[source.get("id")] = source.get("priority", 50)
        return priorities

    def _merge_underlyings(self, symbols, providers, config):
        merged = {}
        priorities = self._provider_priorities(config)
        for symbol in symbols:
            quotes = []
            for provider in providers:
                quote = (provider.get("underlyings") or {}).get(symbol)
                if quote:
                    quotes.append(quote)
            quotes.sort(key=lambda item: (priorities.get(item["provider"], 999), item["provider"]))
            merged[symbol] = {
                "selected": quotes[0] if quotes else None,
                "sources": quotes,
            }
        return merged

    def _merge_options(self, contracts, providers):
        merged = {}
        provider_order = ["alpaca", "cboe", "massive"]
        for contract in contracts:
            quotes = []
            for provider in providers:
                quote = (provider.get("options") or {}).get(contract)
                if quote:
                    quotes.append(quote)
            quotes.sort(key=lambda item: provider_order.index(item["provider"]) if item["provider"] in provider_order else 99)
            merged[contract] = {
                "selected": quotes[0] if quotes else None,
                "sources": quotes,
            }
        return merged

    def _first_ok_provider(self, providers, key, config=None):
        priorities = self._provider_priorities(config or {})
        for provider in sorted(providers, key=lambda item: (priorities.get(item["provider"], 999), item["provider"])):
            if provider.get(key):
                return provider["provider"]
        return None

    def _alpaca_stock_feed(self):
        if not DataFeed:
            return None
        mapping = {
            "iex": DataFeed.IEX,
            "sip": DataFeed.SIP,
            "delayed_sip": DataFeed.DELAYED_SIP,
        }
        return mapping.get(self.stock_feed, DataFeed.IEX)

    def _alpaca_option_feed(self):
        if not OptionsFeed:
            return None
        return OptionsFeed.OPRA if self.option_feed == "opra" else OptionsFeed.INDICATIVE

    def _spread_pct(self, bid, ask, mid):
        if bid and ask and mid:
            return safe_round(((ask - bid) / mid) * 100, 2)
        return None

    def _occ_underlying(self, contract):
        for index, char in enumerate(contract or ""):
            if char.isdigit():
                return contract[:index]
        return None
