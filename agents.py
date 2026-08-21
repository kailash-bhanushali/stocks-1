from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
import json
import math
import os
import re
import statistics
import time
import urllib.parse
import xml.etree.ElementTree as ET

import requests

try:
    from alpaca.data.enums import DataFeed, MarketType, MostActivesBy, OptionsFeed
    from alpaca.data.historical import NewsClient, OptionHistoricalDataClient, ScreenerClient, StockHistoricalDataClient
    from alpaca.data.requests import (
        MarketMoversRequest,
        MostActivesRequest,
        NewsRequest,
        OptionChainRequest,
        StockBarsRequest,
    )
    from alpaca.data.timeframe import TimeFrame
    from alpaca.trading.enums import ContractType
except Exception:
    DataFeed = MarketType = MostActivesBy = OptionsFeed = None
    NewsClient = OptionHistoricalDataClient = ScreenerClient = StockHistoricalDataClient = None
    MarketMoversRequest = MostActivesRequest = NewsRequest = OptionChainRequest = StockBarsRequest = None
    TimeFrame = ContractType = None


HTTP_HEADERS = {
    "User-Agent": os.getenv(
        "RESEARCH_USER_AGENT",
        "trading-research-bot/0.1 contact:kailash-local"
    )
}

POSITIVE_WORDS = {
    "beat", "beats", "surge", "surges", "growth", "raise", "raises", "upgrade",
    "upgraded", "strong", "record", "profit", "profits", "demand", "deal",
    "partnership", "approval", "launch", "expansion", "bullish", "outperform"
}

NEGATIVE_WORDS = {
    "miss", "misses", "drop", "drops", "cut", "cuts", "downgrade", "downgraded",
    "weak", "probe", "lawsuit", "warning", "layoff", "loss", "losses", "bearish",
    "underperform", "recall", "delay", "risk", "slump"
}

SECTOR_ETFS = [
    {"etf": "XLK", "sector": "Technology"},
    {"etf": "XLF", "sector": "Financials"},
    {"etf": "XLE", "sector": "Energy"},
    {"etf": "XLV", "sector": "Healthcare"},
    {"etf": "XLI", "sector": "Industrials"},
    {"etf": "XLC", "sector": "Communication"},
    {"etf": "XLRE", "sector": "Real Estate"},
    {"etf": "XLB", "sector": "Materials"},
    {"etf": "XLP", "sector": "Consumer Staples"},
    {"etf": "XLU", "sector": "Utilities"},
    {"etf": "XLY", "sector": "Consumer Discretionary"},
    {"etf": "SMH", "sector": "Technology"},
    {"etf": "SOXX", "sector": "Technology"},
    {"etf": "CIBR", "sector": "Technology"},
    {"etf": "IGV", "sector": "Technology"},
    {"etf": "IBB", "sector": "Healthcare"},
    {"etf": "URA", "sector": "Energy"},
    {"etf": "GRID", "sector": "Utilities"},
    {"etf": "KRE", "sector": "Financials"},
]

SECTOR_CONSTITUENTS = {
    "AI infrastructure": {
        "sector": "Technology",
        "etfs": ["SMH", "SOXX"],
        "tickers": ["NVDA", "AMD", "AVGO", "TSM", "MU", "ARM", "MRVL", "QCOM", "INTC", "LRCX", "KLAC", "ASML", "AMAT", "ADI", "NXPI"],
        "keywords": ["AI chips", "semiconductor", "GPU", "data center", "AI infrastructure"],
    },
    "Cloud and SaaS": {
        "sector": "Technology",
        "etfs": ["IGV"],
        "tickers": ["MSFT", "NOW", "CRM", "DDOG", "SNOW", "PLTR", "HUBS", "TEAM", "WDAY", "ZS", "MDB", "VEEV", "BILL", "CFLT", "DOCN"],
        "keywords": ["cloud software", "SaaS", "enterprise AI", "cloud computing"],
    },
    "Cybersecurity": {
        "sector": "Technology",
        "etfs": ["CIBR"],
        "tickers": ["PANW", "CRWD", "FTNT", "S", "OKTA", "CYBR", "VRNS", "TENB", "RPD", "NET", "QLYS"],
        "keywords": ["cybersecurity", "zero trust", "endpoint security", "cloud security"],
    },
    "Grid electrification": {
        "sector": "Utilities",
        "etfs": ["XLU", "GRID"],
        "tickers": ["GEV", "ETN", "CEG", "VST", "PWR", "NEE", "DUK", "SO", "AES", "FSLR", "ENPH", "EMR", "ROK", "AME", "WEC"],
        "keywords": ["power demand", "grid electrification", "data center power", "utilities", "clean energy"],
    },
    "Energy and uranium": {
        "sector": "Energy",
        "etfs": ["XLE", "URA"],
        "tickers": ["XOM", "CVX", "CCJ", "UEC", "COP", "EOG", "SLB", "HAL", "OXY", "DVN", "MPC", "VLO", "PSX", "FANG", "AR"],
        "keywords": ["oil", "uranium", "nuclear energy", "energy supply", "natural gas"],
    },
    "Financials and capital markets": {
        "sector": "Financials",
        "etfs": ["XLF", "KRE"],
        "tickers": ["JPM", "GS", "MS", "BAC", "COF", "WFC", "C", "BX", "KKR", "APO", "SCHW", "ICE", "CME", "SPGI", "MCO"],
        "keywords": ["banks", "capital markets", "credit", "rates", "fintech"],
    },
    "Healthcare and biotech": {
        "sector": "Healthcare",
        "etfs": ["XLV", "IBB"],
        "tickers": ["LLY", "UNH", "ABBV", "MRK", "TMO", "JNJ", "PFE", "AMGN", "GILD", "VRTX", "REGN", "ISRG", "BMY", "DHR", "SYK"],
        "keywords": ["drug approval", "GLP-1", "healthcare", "biotech", "FDA"],
    },
    "Consumer discretionary": {
        "sector": "Consumer Discretionary",
        "etfs": ["XLY"],
        "tickers": ["AMZN", "TSLA", "HD", "NKE", "SBUX", "LOW", "TJX", "BKNG", "MCD", "CMG", "LULU", "ROST", "ORLY", "DPZ", "DECK"],
        "keywords": ["consumer spending", "retail", "EV demand", "ecommerce", "luxury"],
    },
    "Industrials and defense": {
        "sector": "Industrials",
        "etfs": ["XLI"],
        "tickers": ["GE", "CAT", "DE", "RTX", "LMT", "NOC", "BA", "HON", "UNP", "UPS", "FDX", "WM", "RSG", "TT", "PH"],
        "keywords": ["defense", "aerospace", "manufacturing", "infrastructure", "industrials"],
    },
    "Communication and media": {
        "sector": "Communication",
        "etfs": ["XLC"],
        "tickers": ["META", "GOOG", "GOOGL", "NFLX", "DIS", "CMCSA", "T", "VZ", "TMUS", "EA", "TTWO", "RBLX", "SPOT", "ROKU"],
        "keywords": ["social media", "streaming", "advertising", "gaming", "telecom"],
    },
    "Materials and mining": {
        "sector": "Materials",
        "etfs": ["XLB"],
        "tickers": ["LIN", "APD", "SHW", "ECL", "FCX", "NEM", "NUE", "STLD", "VMC", "MLM", "CF", "MOS", "CTVA", "DOW", "DD"],
        "keywords": ["gold", "copper", "lithium", "chemicals", "mining", "materials"],
    },
    "Real estate": {
        "sector": "Real Estate",
        "etfs": ["XLRE"],
        "tickers": ["PLD", "AMT", "CCI", "EQIX", "SPG", "O", "PSA", "DLR", "WELL", "AVB", "EQR", "VTR", "ARE", "SBAC", "IRM"],
        "keywords": ["REIT", "data centers", "real estate", "commercial property", "storage"],
    },
    "Consumer staples": {
        "sector": "Consumer Staples",
        "etfs": ["XLP"],
        "tickers": ["PG", "KO", "PEP", "COST", "WMT", "PM", "MO", "CL", "KMB", "GIS", "MNST", "STZ", "EL", "CLX", "HSY"],
        "keywords": ["consumer staples", "grocery", "household", "beverages", "defensive"],
    },
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clamp(value, low=0, high=100):
    return max(low, min(high, value))


def safe_round(value, digits=2):
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except Exception:
        return None


def sentiment_score(texts):
    joined = " ".join(t or "" for t in texts).lower()
    tokens = re.findall(r"[a-zA-Z][a-zA-Z\-']+", joined)
    if not tokens:
        return 50
    positives = sum(1 for token in tokens if token in POSITIVE_WORDS)
    negatives = sum(1 for token in tokens if token in NEGATIVE_WORDS)
    return clamp(50 + (positives - negatives) * 8)


def pct_change(current, previous):
    if current is None or previous in (None, 0):
        return None
    return ((current / previous) - 1) * 100


def average(values):
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains = []
    losses = []
    window = closes[-period - 1:]
    for previous, current in zip(window, window[1:]):
        change = current - previous
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))
    avg_gain = average(gains) or 0
    avg_loss = average(losses) or 0
    if avg_loss == 0:
        return 100 if avg_gain > 0 else 50
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def timed_agent(agent_id, label, fn):
    started = time.perf_counter()
    try:
        payload = fn()
        payload_status = payload.get("status")
        status = "done" if payload_status in (None, "ok") else payload_status
        error = None
    except Exception as exc:
        payload = {}
        status = "error"
        error = f"{type(exc).__name__}: {exc}"
    elapsed = int((time.perf_counter() - started) * 1000)
    run = {
        "id": agent_id,
        "label": label,
        "status": status,
        "duration_ms": elapsed,
        "detail": payload.get("summary", error or "complete")
    }
    if error:
        run["error"] = error
    return run, payload


class OmniRouteClient:
    """
    Reserved for later LLM routing. The current production path is rule-based so
    research scans do not spend tokens by default.
    """
    def __init__(self, endpoint="https://api.omniroute.dev/v1"):
        self.endpoint = endpoint

    def generate_response(self, prompt: str, model: str = "disabled") -> str:
        return json.dumps({
            "model": model,
            "prompt_chars": len(prompt),
            "generated_at": utc_now(),
            "mode": "disabled_for_low_cost"
        })


class YahooChartProvider:
    def fetch_bars(self, symbol, range_value="6mo", interval="1d"):
        encoded = urllib.parse.quote(symbol)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?range={range_value}&interval={interval}"
        response = requests.get(url, headers=HTTP_HEADERS, timeout=12)
        response.raise_for_status()
        data = response.json()
        result = (data.get("chart", {}).get("result") or [None])[0]
        if not result:
            raise RuntimeError(data.get("chart", {}).get("error") or "No chart result")
        timestamps = result.get("timestamp") or []
        quote = (result.get("indicators", {}).get("quote") or [{}])[0]
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []
        bars = []
        for ts, close, volume in zip(timestamps, closes, volumes):
            if close is None:
                continue
            bars.append({
                "date": datetime.fromtimestamp(ts, timezone.utc).date().isoformat(),
                "close": float(close),
                "volume": int(volume or 0),
            })
        if len(bars) < 25:
            raise RuntimeError(f"Only {len(bars)} bars returned")
        return bars


class MarketDataAgent:
    def __init__(self):
        self.yahoo = YahooChartProvider()

    def run(self, symbols):
        metrics = {}
        errors = {}
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(self._symbol_metrics, symbol): symbol for symbol in sorted(set(symbols))}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    metrics[symbol] = future.result()
                except Exception as exc:
                    errors[symbol] = str(exc)
        return {
            "summary": f"Yahoo chart metrics for {len(metrics)}/{len(set(symbols))} symbols",
            "source": "Yahoo Finance chart endpoint",
            "metrics": metrics,
            "errors": errors,
            "status": "degraded" if errors else "ok"
        }

    def _symbol_metrics(self, symbol):
        bars = self.yahoo.fetch_bars(symbol)
        closes = [bar["close"] for bar in bars]
        volumes = [bar["volume"] for bar in bars]
        close = closes[-1]
        sma20 = average(closes[-20:])
        sma50 = average(closes[-50:]) if len(closes) >= 50 else average(closes)
        avg_volume20 = average(volumes[-21:-1]) or average(volumes[-20:])
        volume_ratio = volumes[-1] / avg_volume20 if avg_volume20 else None
        ret5 = pct_change(close, closes[-6]) if len(closes) >= 6 else None
        ret20 = pct_change(close, closes[-21]) if len(closes) >= 21 else None
        ret60 = pct_change(close, closes[-61]) if len(closes) >= 61 else None
        rsi14 = rsi(closes)
        trend_score = 50
        for ret, weight in [(ret5, 1.2), (ret20, 1.5), (ret60, 0.8)]:
            if ret is not None:
                trend_score += ret * weight
        if sma20 and close > sma20:
            trend_score += 8
        if sma50 and close > sma50:
            trend_score += 8
        if volume_ratio and volume_ratio > 1.2:
            trend_score += min(8, (volume_ratio - 1.2) * 8)
        if rsi14 and rsi14 > 75:
            trend_score -= 8
        return {
            "price": safe_round(close),
            "last_date": bars[-1]["date"],
            "return_5d": safe_round(ret5),
            "return_20d": safe_round(ret20),
            "return_60d": safe_round(ret60),
            "sma20": safe_round(sma20),
            "sma50": safe_round(sma50),
            "above_sma20": bool(sma20 and close > sma20),
            "above_sma50": bool(sma50 and close > sma50),
            "volume_ratio": safe_round(volume_ratio),
            "rsi14": safe_round(rsi14),
            "trend_score": int(clamp(trend_score)),
        }


class AlpacaDiscoveryAgent:
    def __init__(self):
        self.api_key = os.getenv("ALPACA_API_KEY", "")
        self.secret_key = os.getenv("ALPACA_SECRET_KEY", "")

    def run(self):
        if not self.api_key or not self.secret_key or ScreenerClient is None:
            return {
                "summary": "Alpaca credentials or SDK unavailable",
                "status": "unavailable",
                "movers": [],
                "actives": [],
                "news": []
            }
        try:
            screener = ScreenerClient(self.api_key, self.secret_key)
            movers = screener.get_market_movers(MarketMoversRequest(top=10, market_type=MarketType.STOCKS))
            actives = screener.get_most_actives(MostActivesRequest(top=10, by=MostActivesBy.VOLUME))
            return {
                "summary": "Alpaca screeners available",
                "status": "ok",
                "movers": self._model_to_list(movers),
                "actives": self._model_to_list(actives),
            }
        except Exception as exc:
            return {
                "summary": "Alpaca screeners unavailable",
                "status": "degraded",
                "error": str(exc)[:300],
                "movers": [],
                "actives": []
            }

    def _model_to_list(self, value):
        if hasattr(value, "model_dump"):
            value = value.model_dump()
        if isinstance(value, dict):
            for key in ("movers", "most_actives", "data"):
                if isinstance(value.get(key), list):
                    return value[key]
        return []


class SectorDiscoveryAgent:
    """Discovers which sectors and themes are active using live ETF momentum data."""

    def __init__(self):
        self.yahoo = YahooChartProvider()

    def run(self, alpaca_discovery=None):
        etf_rankings = self._scan_sector_etfs()
        active_sectors = self._rank_sectors(etf_rankings)
        active_themes = self._build_active_themes(active_sectors)
        alpaca_extras = self._alpaca_mover_extras(alpaca_discovery, active_themes)
        if alpaca_extras:
            active_themes.append({
                "name": "Alpaca momentum",
                "tickers": alpaca_extras,
                "etfs": [],
                "keywords": ["market movers", "high volume", "momentum"],
            })
        return {
            "summary": f"Discovered {len(active_themes)} active themes from {len(active_sectors)} sectors",
            "status": "ok",
            "active_themes": active_themes,
            "alpaca_extras": alpaca_extras,
            "discovery_metadata": {
                "sectors_scanned": len(set(e["etf"] for e in SECTOR_ETFS)),
                "sectors_active": len(active_sectors),
                "active_sector_names": [s["sector"] for s in active_sectors],
                "etf_rankings": etf_rankings[:10],
            },
        }

    def _scan_sector_etfs(self):
        all_etfs = sorted({e["etf"] for e in SECTOR_ETFS})
        rankings = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(self._etf_momentum, etf): etf for etf in all_etfs}
            for future in as_completed(futures):
                etf = futures[future]
                try:
                    metrics = future.result()
                    sector = next((e["sector"] for e in SECTOR_ETFS if e["etf"] == etf), "Unknown")
                    rankings.append({"etf": etf, "sector": sector, **metrics})
                except Exception:
                    pass
        return sorted(rankings, key=lambda x: x.get("momentum_score", 0), reverse=True)

    def _etf_momentum(self, etf):
        bars = self.yahoo.fetch_bars(etf, range_value="3mo", interval="1d")
        closes = [b["close"] for b in bars]
        volumes = [b["volume"] for b in bars]
        ret5 = pct_change(closes[-1], closes[-6]) if len(closes) >= 6 else 0
        ret20 = pct_change(closes[-1], closes[-21]) if len(closes) >= 21 else 0
        avg_vol = average(volumes[-21:-1]) or average(volumes)
        vol_ratio = volumes[-1] / avg_vol if avg_vol else 1
        momentum = (ret5 or 0) * 2.0 + (ret20 or 0) * 1.5 + max(0, (vol_ratio - 1)) * 10
        return {
            "return_5d": safe_round(ret5),
            "return_20d": safe_round(ret20),
            "volume_ratio": safe_round(vol_ratio),
            "momentum_score": safe_round(momentum),
        }

    def _rank_sectors(self, etf_rankings):
        """Pick sectors where at least one ETF shows meaningful momentum."""
        sector_best = {}
        for r in etf_rankings:
            sector = r["sector"]
            score = r.get("momentum_score") or 0
            if sector not in sector_best or score > sector_best[sector]:
                sector_best[sector] = score
        ranked = sorted(
            [{"sector": s, "score": sc} for s, sc in sector_best.items()],
            key=lambda x: x["score"],
            reverse=True,
        )
        active = []
        for s in ranked:
            if s["score"] > -5 or len(active) < 3:
                active.append(s)
            if len(active) >= 5:
                break
        return active

    def _build_active_themes(self, active_sectors):
        """Build theme entries from SECTOR_CONSTITUENTS for active sectors only."""
        active_sector_names = {s["sector"] for s in active_sectors}
        themes = []
        for theme_name, config in SECTOR_CONSTITUENTS.items():
            if config["sector"] in active_sector_names:
                themes.append({
                    "name": theme_name,
                    "tickers": list(config["tickers"]),
                    "etfs": list(config["etfs"]),
                    "keywords": list(config["keywords"]),
                })
        return themes

    def _alpaca_mover_extras(self, alpaca_discovery, active_themes):
        """Return Alpaca movers/actives that are NOT already in any active theme."""
        if not alpaca_discovery:
            return []
        known = set()
        for theme in active_themes:
            known.update(theme["tickers"])
            known.update(theme["etfs"])
        extras = []
        for row in alpaca_discovery.get("movers", []) + alpaca_discovery.get("actives", []):
            sym = (row.get("symbol") or row.get("ticker") or "").upper()
            if sym and sym not in known and sym not in extras:
                extras.append(sym)
        return extras[:15]


class NewsAgent:
    def run(self, themes):
        items_by_theme = {}
        all_titles = []
        errors = {}
        for theme in themes:
            query = f"{theme['name']} stocks " + " ".join(theme["tickers"][:3])
            try:
                items = self._yahoo_news(query)
                items_by_theme[theme["name"]] = items
                all_titles.extend(item["title"] for item in items)
            except Exception as exc:
                errors[theme["name"]] = str(exc)
                items_by_theme[theme["name"]] = []
        return {
            "summary": f"Yahoo news/search returned {sum(len(v) for v in items_by_theme.values())} articles",
            "source": "Yahoo Finance search news",
            "items_by_theme": items_by_theme,
            "sentiment": sentiment_score(all_titles),
            "errors": errors,
            "status": "degraded" if errors else "ok"
        }

    def _yahoo_news(self, query):
        encoded = urllib.parse.quote(query)
        url = f"https://query1.finance.yahoo.com/v1/finance/search?q={encoded}&newsCount=6&quotesCount=0"
        response = requests.get(url, headers=HTTP_HEADERS, timeout=12)
        response.raise_for_status()
        raw_items = response.json().get("news") or []
        items = []
        for item in raw_items[:6]:
            title = item.get("title") or ""
            if not title:
                continue
            items.append({
                "title": title,
                "publisher": item.get("publisher"),
                "link": item.get("link"),
                "published_at": item.get("providerPublishTime"),
            })
        return items


class SocialSentimentAgent:
    def run(self, themes):
        items_by_theme = {}
        all_titles = []
        errors = {}
        for theme in themes:
            query = f"{theme['name']} " + " ".join(theme["tickers"][:2])
            items = []
            for fetcher in (self._reddit_rss, self._hacker_news):
                try:
                    items.extend(fetcher(query))
                except Exception as exc:
                    errors[f"{theme['name']}:{fetcher.__name__}"] = str(exc)[:180]
            deduped = []
            seen = set()
            for item in items:
                key = item["title"].lower()
                if key not in seen:
                    seen.add(key)
                    deduped.append(item)
            items_by_theme[theme["name"]] = deduped[:8]
            all_titles.extend(item["title"] for item in deduped[:8])
        return {
            "summary": f"Social/news-discussion proxy returned {sum(len(v) for v in items_by_theme.values())} posts",
            "source": "Reddit RSS + Hacker News Algolia",
            "items_by_theme": items_by_theme,
            "sentiment": sentiment_score(all_titles),
            "errors": errors,
            "status": "degraded" if errors else "ok"
        }

    def _reddit_rss(self, query):
        encoded = urllib.parse.quote(query)
        url = f"https://www.reddit.com/search.rss?q={encoded}&sort=new&t=week"
        response = requests.get(url, headers=HTTP_HEADERS, timeout=12)
        response.raise_for_status()
        root = ET.fromstring(response.text)
        namespace = {"atom": "http://www.w3.org/2005/Atom"}
        items = []
        for entry in root.findall("atom:entry", namespace)[:5]:
            title = entry.findtext("atom:title", default="", namespaces=namespace)
            link_el = entry.find("atom:link", namespace)
            link = link_el.attrib.get("href") if link_el is not None else None
            if title:
                items.append({"title": title, "source": "Reddit", "link": link})
        return items

    def _hacker_news(self, query):
        encoded = urllib.parse.quote(query)
        url = f"https://hn.algolia.com/api/v1/search?query={encoded}&tags=story&hitsPerPage=5"
        response = requests.get(url, headers=HTTP_HEADERS, timeout=12)
        response.raise_for_status()
        items = []
        for hit in response.json().get("hits", [])[:5]:
            title = hit.get("title") or hit.get("story_title")
            if title:
                items.append({"title": title, "source": "Hacker News", "link": hit.get("url")})
        return items


class FundamentalsAgent:
    def __init__(self):
        self._ticker_map = None

    def run(self, symbols):
        fundamentals = {}
        errors = {}
        for symbol in symbols:
            try:
                fundamentals[symbol] = self._sec_fundamentals(symbol)
            except Exception as exc:
                errors[symbol] = str(exc)[:220]
        return {
            "summary": f"SEC company facts for {len(fundamentals)}/{len(symbols)} symbols",
            "source": "SEC companyfacts API",
            "fundamentals": fundamentals,
            "errors": errors,
            "status": "degraded" if errors else "ok"
        }

    def _load_ticker_map(self):
        if self._ticker_map is not None:
            return self._ticker_map
        url = "https://www.sec.gov/files/company_tickers.json"
        response = requests.get(url, headers=HTTP_HEADERS, timeout=12)
        response.raise_for_status()
        data = response.json()
        self._ticker_map = {
            row["ticker"].upper(): {
                "cik": str(row["cik_str"]).zfill(10),
                "name": row["title"]
            }
            for row in data.values()
        }
        return self._ticker_map

    def _sec_fundamentals(self, symbol):
        ticker_map = self._load_ticker_map()
        if symbol not in ticker_map:
            raise RuntimeError("No SEC CIK mapping")
        company = ticker_map[symbol]
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{company['cik']}.json"
        response = requests.get(url, headers=HTTP_HEADERS, timeout=12)
        response.raise_for_status()
        facts = response.json().get("facts", {}).get("us-gaap", {})
        revenue_growth = self._yoy_growth(facts, [
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "SalesRevenueNet",
        ])
        net_income_growth = self._yoy_growth(facts, ["NetIncomeLoss"])
        score = 50
        if revenue_growth is not None:
            score += clamp(revenue_growth, -25, 25)
        if net_income_growth is not None:
            score += clamp(net_income_growth / 2, -20, 20)
        return {
            "company": company["name"],
            "cik": company["cik"],
            "revenue_yoy": safe_round(revenue_growth),
            "net_income_yoy": safe_round(net_income_growth),
            "fundamental_score": int(clamp(score)),
        }

    def _yoy_growth(self, facts, tags):
        for tag in tags:
            units = facts.get(tag, {}).get("units", {})
            usd = units.get("USD") or []
            annual = [
                item for item in usd
                if item.get("fp") == "FY" and item.get("fy") and item.get("val")
            ]
            annual.sort(key=lambda item: (item.get("fy") or 0, item.get("filed") or ""))
            by_year = {}
            for item in annual:
                by_year[item["fy"]] = item["val"]
            if len(by_year) >= 2:
                years = sorted(by_year)
                latest = by_year[years[-1]]
                previous = by_year[years[-2]]
                return pct_change(latest, previous)
        return None


class OptionsValidationAgent:
    def run(self, candidates, market_metrics):
        validations = {}
        errors = {}
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {
                executor.submit(self._validate_symbol, item["ticker"], item["bias"], market_metrics.get(item["ticker"], {})): item
                for item in candidates
            }
            for future in as_completed(futures):
                item = futures[future]
                symbol = item["ticker"]
                try:
                    validations[symbol] = future.result()
                except Exception as exc:
                    errors[symbol] = str(exc)[:240]
                    validations[symbol] = {
                        "status": "unavailable",
                        "score": 0,
                        "reason": str(exc)[:240],
                        "selected_contract": None,
                    }
        valid_count = sum(1 for value in validations.values() if value["status"] == "validated")
        return {
            "summary": f"Cboe delayed options validated {valid_count}/{len(candidates)} candidates",
            "source": "Cboe delayed options quotes",
            "validations": validations,
            "errors": errors,
            "status": "degraded" if errors else "ok"
        }

    def _validate_symbol(self, symbol, bias, market_metric):
        url = f"https://cdn.cboe.com/api/global/delayed_quotes/options/{urllib.parse.quote(symbol)}.json"
        response = requests.get(url, headers=HTTP_HEADERS, timeout=15)
        response.raise_for_status()
        payload = response.json()
        options = payload.get("data", {}).get("options") or []
        current_price = market_metric.get("price") or payload.get("data", {}).get("current_price")
        today = date.today()
        side_type = "C" if bias == "call" else "P"
        max_premium = float(os.getenv("MAX_OPTION_PREMIUM", "1000"))
        liquid = []
        for option in options:
            parsed = self._parse_occ(option.get("option", ""))
            if not parsed or parsed["type"] != side_type:
                continue
            dte = (parsed["expiration"] - today).days
            if dte < 25 or dte > 65:
                continue
            bid = float(option.get("bid") or 0)
            ask = float(option.get("ask") or 0)
            mid = (bid + ask) / 2 if bid and ask else float(option.get("last_trade_price") or 0)
            if mid <= 0:
                continue
            premium = mid * 100
            if premium > max_premium:
                continue
            spread_pct = ((ask - bid) / mid) if bid and ask and mid else 1
            oi = float(option.get("open_interest") or 0)
            volume = float(option.get("volume") or 0)
            delta = option.get("delta")
            delta_abs = abs(float(delta)) if delta is not None else None
            if spread_pct > 0.25 or oi < 50:
                continue
            contract_score = self._contract_score(dte, delta_abs, spread_pct, oi, volume)
            liquid.append({
                "symbol": option.get("option"),
                "expiration": parsed["expiration"].isoformat(),
                "strike": parsed["strike"],
                "type": "call" if side_type == "C" else "put",
                "dte": dte,
                "bid": safe_round(bid),
                "ask": safe_round(ask),
                "mid": safe_round(mid),
                "premium": safe_round(premium),
                "spread_pct": safe_round(spread_pct * 100),
                "open_interest": int(oi),
                "volume": int(volume),
                "iv": safe_round(option.get("iv")),
                "delta": safe_round(delta),
                "score": int(contract_score),
            })
        liquid.sort(key=lambda item: item["score"], reverse=True)
        if not liquid:
            return {
                "status": "no_liquid_contract",
                "score": 0,
                "reason": "No 25-65 DTE contract passed premium, spread, and open-interest filters.",
                "selected_contract": None,
                "contracts_checked": len(options),
                "timestamp": payload.get("timestamp"),
            }
        selected = liquid[0]
        return {
            "status": "validated" if selected["score"] >= 60 else "weak",
            "score": selected["score"],
            "reason": "Contract passed DTE, premium, spread, and open-interest filters.",
            "selected_contract": selected,
            "contracts_checked": len(options),
            "candidates_found": len(liquid),
            "underlying_price": safe_round(current_price),
            "timestamp": payload.get("timestamp"),
        }

    def _parse_occ(self, symbol):
        match = re.match(r"^([A-Z]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d{8})$", symbol or "")
        if not match:
            return None
        year = 2000 + int(match.group(2))
        month = int(match.group(3))
        day = int(match.group(4))
        strike = int(match.group(6)) / 1000
        return {
            "root": match.group(1),
            "expiration": date(year, month, day),
            "type": match.group(5),
            "strike": strike,
        }

    def _contract_score(self, dte, delta_abs, spread_pct, oi, volume):
        dte_score = clamp(100 - abs(dte - 45) * 2)
        delta_score = 65 if delta_abs is None else clamp(100 - abs(delta_abs - 0.40) * 220)
        spread_score = clamp(100 - spread_pct * 420)
        oi_score = clamp(math.log10(oi + 1) * 24)
        volume_score = clamp(math.log10(volume + 1) * 26)
        return (
            dte_score * 0.20
            + delta_score * 0.25
            + spread_score * 0.25
            + oi_score * 0.20
            + volume_score * 0.10
        )


class ResearcherAgent:
    def __init__(self, llm_client: OmniRouteClient = None):
        self.llm = llm_client or OmniRouteClient()
        self.market_agent = MarketDataAgent()
        self.alpaca_agent = AlpacaDiscoveryAgent()
        self.news_agent = NewsAgent()
        self.social_agent = SocialSentimentAgent()
        self.fundamentals_agent = FundamentalsAgent()
        self.options_agent = OptionsValidationAgent()
        self.discovery_agent = SectorDiscoveryAgent()

    def scan_market(self, market_digest: str = "") -> dict:
        # ── Phase 1: Discovery ─────────────────────────────────
        agent_runs = []

        alpaca_run, alpaca = timed_agent(
            "alpaca", "Alpaca Discovery Agent", self.alpaca_agent.run
        )
        agent_runs.append(alpaca_run)

        discovery_run, discovery = timed_agent(
            "discovery", "Sector Discovery Agent",
            lambda: self.discovery_agent.run(alpaca)
        )
        agent_runs.append(discovery_run)

        active_themes = discovery.get("active_themes", [])
        alpaca_extras = discovery.get("alpaca_extras", [])
        discovery_meta = discovery.get("discovery_metadata", {})

        universe_symbols = sorted({
            symbol
            for theme in active_themes
            for symbol in (theme["tickers"] + theme["etfs"])
        } | set(alpaca_extras) | {"SPY", "QQQ", "IWM", "DIA"})

        # ── Phase 2: Deep Analysis ─────────────────────────────
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(timed_agent, "market", "Market Data Agent", lambda: self.market_agent.run(universe_symbols)): "market",
                executor.submit(timed_agent, "news", "News Agent", lambda: self.news_agent.run(active_themes)): "news",
                executor.submit(timed_agent, "social", "Social Sentiment Agent", lambda: self.social_agent.run(active_themes)): "social",
            }
            results = {"alpaca": alpaca}
            for future in as_completed(futures):
                key = futures[future]
                run, payload = future.result()
                agent_runs.append(run)
                results[key] = payload

        market = results.get("market", {})
        news = results.get("news", {})
        social = results.get("social", {})

        themes = self._rank_themes(market, news, social, active_themes)
        preliminary = self._rank_preliminary_candidates(themes, market, news, social, alpaca, active_themes)
        top_for_deep_checks = preliminary[:14]

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(
                    timed_agent,
                    "fundamentals",
                    "Fundamentals Agent",
                    lambda: self.fundamentals_agent.run([item["ticker"] for item in top_for_deep_checks])
                ): "fundamentals",
                executor.submit(
                    timed_agent,
                    "options",
                    "Options Liquidity Agent",
                    lambda: self.options_agent.run(top_for_deep_checks, market.get("metrics", {}))
                ): "options",
            }
            for future in as_completed(futures):
                key = futures[future]
                run, payload = future.result()
                agent_runs.append(run)
                results[key] = payload

        watchlist = self._build_watchlist(
            preliminary,
            results.get("fundamentals", {}),
            results.get("options", {}),
        )
        debate = self._debate_summary(themes, watchlist, market)
        agent_runs.extend(self._decision_agent_runs(debate, watchlist))

        return {
            "run_id": datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"),
            "generated_at": utc_now(),
            "mode": "dynamic_discovery",
            "universe_size": len(universe_symbols),
            "agent_runs": sorted(agent_runs, key=lambda item: item["id"]),
            "source_health": self._source_health(results),
            "themes": themes,
            "watchlist": watchlist,
            "debate": debate,
            "discovery": {
                "sectors_scanned": discovery_meta.get("sectors_scanned", 0),
                "sectors_active": discovery_meta.get("sectors_active", 0),
                "themes_active": len(active_themes),
                "alpaca_extras": alpaca_extras,
                "etf_rankings": discovery_meta.get("etf_rankings", []),
            },
            "raw_inputs": {
                "market": {
                    "source": market.get("source"),
                    "symbols_loaded": len(market.get("metrics", {})),
                    "errors": market.get("errors", {}),
                },
                "news": {
                    "source": news.get("source"),
                    "sentiment": news.get("sentiment"),
                    "errors": news.get("errors", {}),
                },
                "social": {
                    "source": social.get("source"),
                    "sentiment": social.get("sentiment"),
                    "errors": social.get("errors", {}),
                },
                "alpaca": {
                    "status": alpaca.get("status"),
                    "error": alpaca.get("error"),
                },
                "options": {
                    "source": results.get("options", {}).get("source"),
                    "errors": results.get("options", {}).get("errors", {}),
                },
            },
            "token_policy": {
                "research_model": "none",
                "final_decision_model": "none",
                "reason": "Rule-based pipeline uses live data sources first; LLM routing is disabled by default for low cost."
            }
        }

    def _rank_themes(self, market, news, social, active_themes):
        metrics = market.get("metrics", {})
        spy_return = (metrics.get("SPY") or {}).get("return_20d") or 0
        ranked = []
        for theme in active_themes:
            ticker_metrics = [metrics.get(symbol, {}) for symbol in theme["tickers"] + theme["etfs"]]
            ret5 = average([item.get("return_5d") for item in ticker_metrics])
            ret20 = average([item.get("return_20d") for item in ticker_metrics])
            trend = average([item.get("trend_score") for item in ticker_metrics])
            volume = average([item.get("volume_ratio") for item in ticker_metrics])
            news_items = news.get("items_by_theme", {}).get(theme["name"], [])
            social_items = social.get("items_by_theme", {}).get(theme["name"], [])
            news_score = sentiment_score([item.get("title") for item in news_items])
            social_score = sentiment_score([item.get("title") for item in social_items])
            relative = (ret20 or 0) - spy_return
            strength = clamp(
                45
                + (trend or 50) * 0.28
                + relative * 1.4
                + (ret5 or 0) * 1.0
                + ((volume or 1) - 1) * 8
                + (news_score - 50) * 0.18
                + (social_score - 50) * 0.12
            )
            direction = "bullish" if strength >= 62 and (ret20 or 0) >= -2 else "bearish" if strength <= 42 else "mixed"
            top_tickers = sorted(
                theme["tickers"],
                key=lambda symbol: (metrics.get(symbol, {}).get("trend_score") or 0),
                reverse=True
            )[:4]
            evidence = []
            if ret20 is not None:
                evidence.append(f"20D basket return {ret20:.1f}% vs SPY {spy_return:.1f}%")
            if volume is not None:
                evidence.append(f"Average relative volume {volume:.2f}x")
            if news_items:
                evidence.append(f"{len(news_items)} recent news items")
            if social_items:
                evidence.append(f"{len(social_items)} social/news-discussion hits")
            ranked.append({
                "name": theme["name"],
                "strength": int(strength),
                "direction": direction,
                "why": "; ".join(evidence[:3]) or "Insufficient evidence",
                "tickers": top_tickers,
                "metrics": {
                    "return_5d": safe_round(ret5),
                    "return_20d": safe_round(ret20),
                    "relative_to_spy_20d": safe_round(relative),
                    "volume_ratio": safe_round(volume),
                    "news_score": int(news_score),
                    "social_score": int(social_score),
                },
                "evidence": evidence,
            })
        return sorted(ranked, key=lambda item: item["strength"], reverse=True)

    def _rank_preliminary_candidates(self, themes, market, news, social, alpaca, active_themes):
        metrics = market.get("metrics", {})
        theme_by_name = {theme["name"]: theme for theme in themes}
        theme_config_by_name = {theme["name"]: theme for theme in active_themes}
        active_symbols = self._alpaca_symbols(alpaca)
        candidates = []
        for theme in themes:
            config = theme_config_by_name[theme["name"]]
            for symbol in config["tickers"]:
                symbol_metrics = metrics.get(symbol)
                if not symbol_metrics:
                    continue
                news_items = news.get("items_by_theme", {}).get(theme["name"], [])
                social_items = social.get("items_by_theme", {}).get(theme["name"], [])
                news_score = sentiment_score([item.get("title") for item in news_items])
                social_score = sentiment_score([item.get("title") for item in social_items])
                active_bonus = 5 if symbol in active_symbols else 0
                score = clamp(
                    theme["strength"] * 0.30
                    + symbol_metrics["trend_score"] * 0.42
                    + news_score * 0.12
                    + social_score * 0.08
                    + active_bonus
                )
                bias = "call" if score >= 55 and theme["direction"] != "bearish" else "put"
                trigger = self._trigger_text(symbol_metrics, bias)
                candidates.append({
                    "ticker": symbol,
                    "theme": theme["name"],
                    "bias": bias,
                    "pre_options_score": int(score),
                    "theme_score": theme["strength"],
                    "technical_score": symbol_metrics["trend_score"],
                    "news_score": int(news_score),
                    "social_score": int(social_score),
                    "price": symbol_metrics["price"],
                    "trigger": trigger,
                    "confirmation": self._confirmation_rules(bias),
                    "evidence": [
                        f"{theme['name']} strength {theme['strength']}/100",
                        f"{symbol} trend score {symbol_metrics['trend_score']}/100",
                        f"5D {symbol_metrics.get('return_5d')}%, 20D {symbol_metrics.get('return_20d')}%",
                    ],
                })
        return sorted(candidates, key=lambda item: item["pre_options_score"], reverse=True)

    def _build_watchlist(self, preliminary, fundamentals, options):
        fundamental_by_symbol = fundamentals.get("fundamentals", {})
        option_by_symbol = options.get("validations", {})
        watchlist = []
        for item in preliminary:
            option_validation = option_by_symbol.get(item["ticker"], {
                "status": "not_checked",
                "score": 0,
                "reason": "Candidate was below deep-check cutoff.",
                "selected_contract": None,
            })
            if option_validation["status"] not in {"validated", "weak"}:
                continue
            fundamental = fundamental_by_symbol.get(item["ticker"], {})
            fundamental_score = fundamental.get("fundamental_score", 50)
            score = clamp(
                item["pre_options_score"] * 0.62
                + option_validation.get("score", 0) * 0.25
                + fundamental_score * 0.13
            )
            risk_notes = []
            selected_contract = option_validation.get("selected_contract")
            if selected_contract:
                if selected_contract["spread_pct"] > 18:
                    risk_notes.append("Wide option spread")
                if selected_contract["premium"] > float(os.getenv("MAX_OPTION_PREMIUM", "1000")) * 0.8:
                    risk_notes.append("Premium near max risk cap")
            if score < 65:
                risk_notes.append("Score is watch-only until stronger confirmation")
            enriched = dict(item)
            enriched.update({
                "score": int(score),
                "fundamental_score": int(fundamental_score),
                "fundamentals": fundamental,
                "options": option_validation,
                "risk_notes": risk_notes,
            })
            watchlist.append(enriched)
        return sorted(watchlist, key=lambda item: item["score"], reverse=True)[:8]

    def _debate_summary(self, themes, watchlist, market):
        top_theme = themes[0] if themes else {}
        spy = market.get("metrics", {}).get("SPY", {})
        qqq = market.get("metrics", {}).get("QQQ", {})
        bull_points = []
        bear_points = []
        if top_theme:
            bull_points.append(f"Top theme is {top_theme['name']} with strength {top_theme['strength']}/100.")
        for item in watchlist[:3]:
            contract = item.get("options", {}).get("selected_contract")
            if contract:
                bull_points.append(
                    f"{item['ticker']} has validated {contract['dte']} DTE {contract['type']} liquidity with {contract['open_interest']} OI."
                )
            if item.get("risk_notes"):
                bear_points.extend(f"{item['ticker']}: {note}" for note in item["risk_notes"])
        if spy.get("above_sma50") is False:
            bear_points.append("SPY is below its 50D average, so long premium needs tighter confirmation.")
        if qqq.get("rsi14") and qqq["rsi14"] > 75:
            bear_points.append("QQQ is overbought on 14D RSI.")
        if not bear_points:
            bear_points.append("No severe portfolio-level risk flags in available data.")
        manager = "Watchlist ready; wait for verified TradingView alert before any order."
        if not watchlist:
            manager = "No option-validated watchlist from current data."
        return {
            "bull": bull_points[:5],
            "bear": bear_points[:5],
            "risk": {
                "market_regime": "risk_on" if spy.get("above_sma50") else "caution",
                "spy_trend_score": spy.get("trend_score"),
                "qqq_trend_score": qqq.get("trend_score"),
                "max_option_premium": float(os.getenv("MAX_OPTION_PREMIUM", "1000")),
            },
            "manager": manager,
        }

    def _decision_agent_runs(self, debate, watchlist):
        return [
            {
                "id": "bull",
                "label": "Bull Researcher",
                "status": "done",
                "duration_ms": 0,
                "detail": debate["bull"][0] if debate["bull"] else "No bullish evidence",
            },
            {
                "id": "bear",
                "label": "Bear Researcher",
                "status": "done",
                "duration_ms": 0,
                "detail": debate["bear"][0] if debate["bear"] else "No bearish evidence",
            },
            {
                "id": "risk",
                "label": "Risk Manager",
                "status": "done",
                "duration_ms": 0,
                "detail": f"Market regime: {debate['risk']['market_regime']}",
            },
            {
                "id": "manager",
                "label": "Manager",
                "status": "done" if watchlist else "blocked",
                "duration_ms": 0,
                "detail": debate["manager"],
            },
        ]

    def _source_health(self, results):
        health = []
        for key, label in [
            ("market", "Yahoo chart"),
            ("alpaca", "Alpaca data"),
            ("news", "Yahoo news"),
            ("social", "Social proxy"),
            ("fundamentals", "SEC facts"),
            ("options", "Cboe options"),
        ]:
            payload = results.get(key, {})
            health.append({
                "name": label,
                "status": payload.get("status", "unknown"),
                "detail": payload.get("summary") or payload.get("error") or "not run",
            })
        return health

    def _alpaca_symbols(self, alpaca):
        symbols = set()
        for row in alpaca.get("movers", []) + alpaca.get("actives", []):
            if isinstance(row, dict):
                symbol = row.get("symbol") or row.get("ticker")
                if symbol:
                    symbols.add(symbol.upper())
        return symbols

    def _trigger_text(self, metrics, bias):
        price = metrics.get("price")
        vol = metrics.get("volume_ratio")
        rsi = metrics.get("rsi14")
        above_50 = metrics.get("above_sma50")
        parts = []
        if price:
            parts.append(f"Price ${price:.2f}")
        if vol:
            parts.append(f"{vol:.1f}x Vol Surge")
        if rsi:
            parts.append(f"RSI {rsi:.0f}")
        if above_50 is not None:
            parts.append("Above 50D SMA" if above_50 else "Below 50D SMA")
        condition_str = ", ".join(parts) if parts else "Trend continuation"
        if bias == "call":
            return f"BUY Alert on TV: {condition_str} (Breakout Target)"
        return f"SELL Alert on TV: {condition_str} (Breakdown Target)"

    def _confirmation_rules(self, bias):
        if bias == "call":
            return [
                "price above rising 20D/50D trend",
                "weekly trend filter remains bullish",
                "alert action is buy/long/call",
                "broad market filter is not risk-off",
            ]
        return [
            "price below falling 20D/50D trend",
            "weekly trend filter remains bearish",
            "alert action is sell/short/put",
            "broad market filter supports downside hedge",
        ]


class TechnicalConfirmationAgent:
    def evaluate_signal(self, signal: dict, research_context: dict) -> dict:
        ticker = str(signal.get("ticker", "")).upper()
        action = str(signal.get("action", "")).lower()
        source = signal.get("source") or "unknown"
        verified = bool(signal.get("verified"))
        signal_type = str(signal.get("signal_type") or "").upper()
        trend = str(signal.get("trend") or "").lower()
        watch = next(
            (item for item in research_context.get("watchlist", []) if item["ticker"] == ticker),
            None
        )
        if not verified and source == "tradingview":
            return {
                "status": "rejected",
                "ticker": ticker,
                "source": source,
                "reason": "TradingView webhook was not verified with TRADINGVIEW_WEBHOOK_SECRET.",
                "confidence": 0.05
            }
        if not watch:
            return {
                "status": "rejected",
                "ticker": ticker,
                "source": source,
                "reason": "Signal ticker is not in the option-validated research watchlist.",
                "confidence": 0.15
            }
        if signal_type == "ZONE_UPDATE":
            expected_trend = "bullish" if watch["bias"] == "call" else "bearish"
            aligned = trend == expected_trend
            return {
                "status": "state_update",
                "ticker": ticker,
                "source": source,
                "verified": verified,
                "theme": watch["theme"],
                "bias": watch["bias"],
                "trend": trend,
                "reason": (
                    f"Zone Shift state aligns with {watch['bias']} bias; waiting for actionable BUY/SELL alert."
                    if aligned
                    else f"Zone Shift trend '{trend}' does not align with {watch['bias']} bias yet."
                ),
                "confidence": 0.55 if aligned else 0.25,
                "is_real_tradingview": source == "tradingview" and verified,
            }
        bullish_actions = {"buy", "long", "call", "bullish"}
        bearish_actions = {"sell", "short", "put", "bearish"}
        expected = bullish_actions if watch["bias"] == "call" else bearish_actions
        if action not in expected:
            return {
                "status": "rejected",
                "ticker": ticker,
                "source": source,
                "theme": watch["theme"],
                "reason": f"Signal action '{action}' does not align with {watch['bias']} bias.",
                "confidence": 0.30
            }
        confidence = clamp(watch["score"] * 0.72 + watch["options"].get("score", 0) * 0.28) / 100
        return {
            "status": "confirmed",
            "ticker": ticker,
            "source": source,
            "verified": verified,
            "theme": watch["theme"],
            "bias": watch["bias"],
            "matched_trigger": watch["trigger"],
            "contract": watch.get("options", {}).get("selected_contract"),
            "confidence": safe_round(confidence, 3),
            "is_real_tradingview": source == "tradingview" and verified,
        }


class TraderAgent:
    def __init__(self, llm_client: OmniRouteClient = None):
        self.llm = llm_client or OmniRouteClient()

    def evaluate_trade(self, webhook_signal: dict, research_context: dict, technical_context: dict) -> dict:
        ticker = str(webhook_signal.get("ticker", "")).upper()
        watch = next(
            (item for item in research_context.get("watchlist", []) if item["ticker"] == ticker),
            None
        )
        if technical_context.get("status") != "confirmed" or not watch:
            return {
                "decision": "skip",
                "confidence": technical_context.get("confidence", 0),
                "reason": technical_context.get("reason", "Technical confirmation failed."),
                "risk_checks": [],
            }
        contract = watch.get("options", {}).get("selected_contract")
        risk_checks = self._risk_checks(watch, contract, research_context)
        failed = [check for check in risk_checks if check["status"] == "fail"]
        warnings = [check for check in risk_checks if check["status"] == "warn"]
        if failed:
            return {
                "decision": "skip",
                "confidence": technical_context.get("confidence", 0),
                "reason": failed[0]["message"],
                "risk_checks": risk_checks,
                "contract_plan": {"instrument": "option", "symbol": contract.get("symbol") if contract else None}
            }
        confidence = float(technical_context.get("confidence") or 0)
        if warnings:
            confidence = max(0, confidence - 0.08 * len(warnings))
        is_real_tradingview = bool(technical_context.get("is_real_tradingview"))
        return {
            "decision": "approved_for_paper_order" if is_real_tradingview else "test_plan_only",
            "confidence": safe_round(confidence, 3),
            "reason": (
                "Research, options liquidity, risk checks, and verified TradingView confirmation agree."
                if is_real_tradingview
                else "Local test signal matched research and options checks; wait for verified TradingView before real paper approval."
            ),
            "risk_checks": risk_checks,
            "contract_plan": {
                "instrument": "option",
                "symbol": contract.get("symbol") if contract else None,
                "type": contract.get("type") if contract else watch["bias"],
                "expiration": contract.get("expiration") if contract else None,
                "strike": contract.get("strike") if contract else None,
                "premium": contract.get("premium") if contract else None,
                "max_contracts": 1,
                "execution_phase": "disabled_until_alpaca_phase",
            }
        }

    def _risk_checks(self, watch, contract, research_context):
        checks = []
        if not contract:
            checks.append({"name": "contract", "status": "fail", "message": "No validated options contract selected."})
            return checks
        max_premium = float(os.getenv("MAX_OPTION_PREMIUM", "1000"))
        checks.append({
            "name": "premium_cap",
            "status": "pass" if contract["premium"] <= max_premium else "fail",
            "message": f"Premium ${contract['premium']} vs max ${max_premium:.0f}",
        })
        checks.append({
            "name": "spread",
            "status": "pass" if contract["spread_pct"] <= 18 else "warn" if contract["spread_pct"] <= 25 else "fail",
            "message": f"Bid/ask spread {contract['spread_pct']}%",
        })
        checks.append({
            "name": "open_interest",
            "status": "pass" if contract["open_interest"] >= 100 else "warn",
            "message": f"Open interest {contract['open_interest']}",
        })
        checks.append({
            "name": "dte",
            "status": "pass" if 25 <= contract["dte"] <= 65 else "fail",
            "message": f"{contract['dte']} DTE",
        })
        regime = research_context.get("debate", {}).get("risk", {}).get("market_regime")
        checks.append({
            "name": "market_regime",
            "status": "pass" if regime == "risk_on" else "warn",
            "message": f"Market regime {regime or 'unknown'}",
        })
        checks.append({
            "name": "watchlist_score",
            "status": "pass" if watch["score"] >= 68 else "warn",
            "message": f"Watchlist score {watch['score']}/100",
        })
        return checks
