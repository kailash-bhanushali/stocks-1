from concurrent.futures import ThreadPoolExecutor, as_completed
import copy
from datetime import date, datetime, timedelta, timezone
import json
import math
import os
import re
import statistics
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET

import requests

from intelligence_agent import compute_intelligence_scoring, fetch_all_intelligence
from scoring_calibration import CalibrationError, calibrate as calibrate_scoring_weights

from source_adapters import (
    CustomSourceRequestError,
    CustomSourceValidationError,
    GenericJsonSource,
    contracts_for_ui,
    credential_state as custom_credential_state,
    validate_custom_source,
)

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

SOURCE_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "runtime", "sources_config.json")
SOURCE_CONFIG_LOCK = threading.RLock()

LEGACY_SCORING_PROVENANCE = {
    "status": "legacy_unvalidated",
    "method": "Hand-tuned heuristic inherited from the original repository",
    "origin": "No experiment, backtest, or statistical fit was recorded for these defaults.",
    "target": None,
    "generated_at": None,
    "metrics": {},
    "limitations": [
        "The default coefficients are starting assumptions, not evidence of predictive performance.",
        "Use Calibrate & Apply to fit the weights on point-in-time historical bars and inspect holdout metrics.",
    ],
}


def _field(label, field_type, help_text, **constraints):
    return {"label": label, "type": field_type, "help": help_text, **constraints}


DEFAULT_SOURCES_CONFIG = {
    "discovery": {
        "title": "Discovery: sectors, ETFs, and movers",
        "description": "Ranks configured sector ETFs with Yahoo daily bars, then optionally adds Alpaca top movers and most-active symbols. Changes apply to the next scan without restarting the server.",
        "source_inventory": [
            {"id": "yahoo_chart", "label": "Yahoo Finance Chart", "purpose": "ETF price, return, and volume momentum", "endpoint_field": "yahoo_chart_endpoint"},
            {"id": "alpaca_screener", "label": "Alpaca Screener", "purpose": "US stock movers and most-active symbols", "credential_env": ["ALPACA_API_KEY", "ALPACA_SECRET_KEY"]},
        ],
        "custom_sources": [],
        "enabled_sources": ["yahoo_chart", "alpaca_screener"],
        "sector_etfs": [
            "XLK:Technology", "XLF:Financials", "XLE:Energy", "XLV:Healthcare",
            "XLI:Industrials", "XLC:Communication", "XLRE:Real Estate", "XLB:Materials",
            "XLP:Consumer Staples", "XLU:Utilities", "XLY:Consumer Discretionary",
            "SMH:Technology", "SOXX:Technology", "CIBR:Technology", "IGV:Technology",
            "IBB:Healthcare", "URA:Energy", "GRID:Utilities", "KRE:Financials",
        ],
        "benchmark_lookback_range": "3mo",
        "chart_interval": "1d",
        "top_sectors_count": 5,
        "minimum_sectors_count": 3,
        "minimum_momentum_score": -5.0,
        "weight_5d_return": 2.0,
        "weight_20d_return": 1.5,
        "weight_volume_expansion": 10.0,
        "calibration_lookback_range": "2y",
        "calibration_target_days": 5,
        "calibration_ridge_penalty": 5.0,
        "scoring_provenance": copy.deepcopy(LEGACY_SCORING_PROVENANCE),
        "alpaca_movers_count": 10,
        "alpaca_actives_count": 10,
        "alpaca_extra_limit": 15,
        "request_timeout_seconds": 12,
        "yahoo_chart_endpoint": "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
        "field_schema": {
            "custom_sources": _field("Custom discovery sources", "source_list", "Managed through Add Source.", hidden=True),
            "enabled_sources": _field("Enabled discovery sources", "list", "Comma-separated source IDs. Yahoo provides ETF momentum; Alpaca adds movers. Allowed: yahoo_chart, alpaca_screener.", choices=["yahoo_chart", "alpaca_screener"]),
            "sector_etfs": _field("Sector ETFs", "list", "Comma-separated TICKER:Sector entries. New ETFs may be added; ticker-only entries are grouped under Custom.", min_items=1, max_items=100),
            "benchmark_lookback_range": _field("ETF chart lookback", "text", "Yahoo range such as 1mo, 3mo, 6mo, 1y, or 2y.", placeholder="3mo"),
            "chart_interval": _field("ETF bar interval", "text", "Yahoo interval such as 1d or 1wk. Daily bars are expected by the momentum calculation.", placeholder="1d"),
            "top_sectors_count": _field("Maximum active sectors", "integer", "Highest-ranked sectors allowed into theme discovery.", min=1, max=20),
            "minimum_sectors_count": _field("Minimum active sectors", "integer", "Always retain at least this many sectors, even when momentum is below the cutoff.", min=1, max=20),
            "minimum_momentum_score": _field("Momentum cutoff", "number", "A sector above this score can be selected after the minimum count is satisfied.", min=-100, max=100),
            "weight_5d_return": _field("5-day return weight", "number", "Score points per 1 percentage point of 5-day ETF return.", min=-20, max=20, origin="Legacy hand-tuned default; no recorded empirical basis until calibrated."),
            "weight_20d_return": _field("20-day return weight", "number", "Score points per 1 percentage point of 20-day ETF return.", min=-20, max=20, origin="Legacy hand-tuned default; no recorded empirical basis until calibrated."),
            "weight_volume_expansion": _field("Volume-expansion weight", "number", "Score points per 1.0 of volume ratio above its 20-day average; below-average volume adds zero.", min=-100, max=100, origin="Legacy hand-tuned default; no recorded empirical basis until calibrated."),
            "calibration_lookback_range": _field("Calibration history", "text", "Yahoo range used when fitting score weights, such as 1y, 2y, or 5y. Use at least 2y for a useful holdout.", placeholder="2y"),
            "calibration_target_days": _field("Calibration target horizon", "integer", "Forward trading-day return the regression tries to predict.", min=1, max=20),
            "calibration_ridge_penalty": _field("Calibration regularization", "number", "Ridge penalty that reduces unstable coefficients; larger values shrink weights more.", min=0.01, max=1000),
            "scoring_provenance": _field("Scoring provenance", "object", "Generated calibration evidence shown in the scoring explanation card.", hidden=True),
            "alpaca_movers_count": _field("Alpaca movers requested", "integer", "Number requested from Alpaca's market-movers screener.", min=1, max=50),
            "alpaca_actives_count": _field("Alpaca most-active requested", "integer", "Number requested from Alpaca's volume screener.", min=1, max=50),
            "alpaca_extra_limit": _field("Extra mover symbol limit", "integer", "Maximum unique Alpaca symbols added outside configured themes.", min=0, max=50),
            "request_timeout_seconds": _field("HTTP timeout (seconds)", "number", "Per-request timeout for Yahoo ETF charts.", min=2, max=60),
            "yahoo_chart_endpoint": _field("Yahoo chart endpoint", "url", "Full HTTP(S) URL template. It must contain {symbol}; range and interval are sent as query parameters.", placeholder="https://.../{symbol}"),
        },
    },
    "market": {
        "title": "Market: technical bars and live quote router",
        "description": "Builds technical metrics from Yahoo bars. After watchlist selection, the read-only quote router checks enabled Alpaca, Cboe, Finnhub, Alpha Vantage, and Massive sources.",
        "source_inventory": [
            {"id": "yahoo_chart", "label": "Yahoo Finance Chart", "purpose": "Historical bars used for price, volume, SMA, RSI, and returns", "endpoint_field": "yahoo_chart_endpoint"},
            {"id": "alpaca", "label": "Alpaca Market Data", "purpose": "Stock and selected-option snapshots", "credential_env": ["ALPACA_API_KEY", "ALPACA_SECRET_KEY"]},
            {"id": "cboe", "label": "Cboe Delayed Options", "purpose": "Option-chain liquidity validation and delayed selected-option quotes", "endpoint_field": "cboe_endpoint"},
            {"id": "finnhub", "label": "Finnhub", "purpose": "Underlying stock quote fallback", "credential_env": ["FINNHUB_API_KEY"], "endpoint_field": "finnhub_quote_endpoint"},
            {"id": "alpha_vantage", "label": "Alpha Vantage", "purpose": "Underlying stock quote fallback", "credential_env": ["ALPHA_VANTAGE_API_KEY"], "endpoint_field": "alpha_vantage_endpoint"},
            {"id": "massive", "label": "Massive / Polygon", "purpose": "Underlying stock snapshot fallback", "credential_env": ["MASSIVE_API_KEY|POLYGON_API_KEY"], "endpoint_field": "massive_endpoint"},
        ],
        "custom_sources": [],
        "enabled_sources": ["yahoo_chart", "alpaca", "cboe", "finnhub", "alpha_vantage", "massive"],
        "benchmark_symbols": ["SPY", "QQQ", "IWM", "DIA"],
        "chart_lookback_range": "6mo",
        "chart_interval": "1d",
        "sma_short_days": 20,
        "sma_long_days": 50,
        "rsi_days": 14,
        "minimum_volume_ratio": 1.2,
        "overbought_rsi": 75.0,
        "weight_5d_return": 1.2,
        "weight_20d_return": 1.5,
        "weight_60d_return": 0.8,
        "sma_short_bonus": 8.0,
        "sma_long_bonus": 8.0,
        "weight_volume_expansion": 8.0,
        "volume_bonus_cap": 8.0,
        "overbought_penalty": 8.0,
        "calibration_symbols": ["SPY", "QQQ", "IWM", "DIA", "XLK", "XLF", "XLE", "XLV", "XLI", "XLY"],
        "calibration_lookback_range": "2y",
        "calibration_target_days": 5,
        "calibration_ridge_penalty": 5.0,
        "scoring_provenance": copy.deepcopy(LEGACY_SCORING_PROVENANCE),
        "max_parallel_requests": 8,
        "request_timeout_seconds": 12,
        "max_quote_symbols": 8,
        "max_quote_contracts": 8,
        "yahoo_chart_endpoint": "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
        "finnhub_quote_endpoint": "https://finnhub.io/api/v1/quote",
        "alpha_vantage_endpoint": "https://www.alphavantage.co/query",
        "massive_endpoint": "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}",
        "cboe_endpoint": "https://cdn.cboe.com/api/global/delayed_quotes/options/{underlying}.json",
        "unusual_flow": {
            "min_vol_oi_ratio": 3.0,
            "min_premium_dollars": 10000,
            "min_open_interest": 50,
            "max_dte": 90,
        },
        "field_schema": {
            "custom_sources": _field("Custom market sources", "source_list", "Managed through Add Source.", hidden=True),
            "enabled_sources": _field("Enabled market sources", "list", "Comma-separated source IDs. yahoo_chart is the technical-bar source; the others are live quote-router fallbacks.", choices=["yahoo_chart", "alpaca", "cboe", "finnhub", "alpha_vantage", "massive"]),
            "benchmark_symbols": _field("Benchmark symbols", "list", "Symbols always added to the technical scan for market-regime and relative-strength comparisons.", min_items=1, max_items=20),
            "chart_lookback_range": _field("Technical chart lookback", "text", "Yahoo range. It must provide enough bars for the long SMA and 60-day return.", placeholder="6mo"),
            "chart_interval": _field("Technical bar interval", "text", "Yahoo interval; use 1d for the day-based calculations shown in the UI.", placeholder="1d"),
            "sma_short_days": _field("Short SMA days", "integer", "Number of most recent closes in the short simple moving average.", min=2, max=250),
            "sma_long_days": _field("Long SMA days", "integer", "Number of most recent closes in the long simple moving average.", min=3, max=500),
            "rsi_days": _field("RSI period", "integer", "Number of daily changes used in RSI.", min=2, max=100),
            "minimum_volume_ratio": _field("Volume surge threshold", "number", "Latest volume divided by prior average. Example: 1.2 means 20% above average.", min=0, max=20),
            "overbought_rsi": _field("Overbought RSI cutoff", "number", "RSI above this level receives the configured penalty.", min=1, max=100),
            "weight_5d_return": _field("5-day return weight", "number", "Score points per 1 percentage point of 5-day return.", min=-20, max=20, origin="Legacy hand-tuned default; no recorded empirical basis until calibrated."),
            "weight_20d_return": _field("20-day return weight", "number", "Score points per 1 percentage point of 20-day return.", min=-20, max=20, origin="Legacy hand-tuned default; no recorded empirical basis until calibrated."),
            "weight_60d_return": _field("60-day return weight", "number", "Score points per 1 percentage point of 60-day return.", min=-20, max=20, origin="Legacy hand-tuned default; no recorded empirical basis until calibrated."),
            "sma_short_bonus": _field("Above-short-SMA bonus", "number", "Points added when price is above the configured short SMA.", min=-50, max=50, origin="Legacy hand-tuned default; calibration treats this as a binary feature."),
            "sma_long_bonus": _field("Above-long-SMA bonus", "number", "Points added when price is above the configured long SMA.", min=-50, max=50, origin="Legacy hand-tuned default; calibration treats this as a binary feature."),
            "weight_volume_expansion": _field("Volume-expansion weight", "number", "Points per 1.0 of volume ratio above the surge threshold, before the cap.", min=-100, max=100, origin="Legacy hand-tuned default; calibration fits volume expansion as a continuous feature."),
            "volume_bonus_cap": _field("Volume bonus cap", "number", "Maximum points added for volume above the configured threshold.", min=0, max=50),
            "overbought_penalty": _field("Overbought penalty", "number", "Points removed when RSI exceeds the cutoff.", min=0, max=50, origin="Legacy hand-tuned default; calibration treats overbought as a binary feature."),
            "calibration_symbols": _field("Calibration symbols", "list", "Comma-separated liquid benchmarks and sector ETFs used to fit and validate market weights.", min_items=4, max_items=50),
            "calibration_lookback_range": _field("Calibration history", "text", "Yahoo range used when fitting score weights, such as 1y, 2y, or 5y.", placeholder="2y"),
            "calibration_target_days": _field("Calibration target horizon", "integer", "Forward trading-day return the regression tries to predict.", min=1, max=20),
            "calibration_ridge_penalty": _field("Calibration regularization", "number", "Ridge penalty that reduces unstable coefficients; larger values shrink weights more.", min=0.01, max=1000),
            "scoring_provenance": _field("Scoring provenance", "object", "Generated calibration evidence shown in the scoring explanation card.", hidden=True),
            "max_parallel_requests": _field("Parallel chart requests", "integer", "Maximum concurrent Yahoo bar requests.", min=1, max=32),
            "request_timeout_seconds": _field("HTTP timeout (seconds)", "number", "Per-request timeout for chart and quote HTTP calls.", min=2, max=60),
            "max_quote_symbols": _field("Live underlying quote limit", "integer", "Maximum watchlist underlyings sent to the live quote router.", min=1, max=50),
            "max_quote_contracts": _field("Live option quote limit", "integer", "Maximum selected option contracts sent to the live quote router.", min=1, max=50),
            "yahoo_chart_endpoint": _field("Yahoo chart endpoint", "url", "HTTP(S) URL template containing {symbol}.", placeholder="https://.../{symbol}"),
            "finnhub_quote_endpoint": _field("Finnhub quote endpoint", "url", "HTTP(S) quote endpoint; symbol and token are query parameters."),
            "alpha_vantage_endpoint": _field("Alpha Vantage endpoint", "url", "HTTP(S) query endpoint; function, symbol, and API key are query parameters."),
            "massive_endpoint": _field("Massive snapshot endpoint", "url", "HTTP(S) URL template containing {symbol}."),
            "cboe_endpoint": _field("Cboe options endpoint", "url", "HTTP(S) URL template containing {underlying}."),
            "unusual_flow": _field("Unusual options flow config", "object", "Thresholds for the Vol/OI unusual flow scanner. Adjust min_vol_oi_ratio (default 3.0) and min_premium_dollars (default 10000) to tune sensitivity."),
        },
    },
    "news": {
        "title": "News: catalysts and headline sentiment",
        "description": "Searches each active theme on enabled endpoints, de-duplicates headlines, and scores configurable positive and negative terms.",
        "source_inventory": [
            {"id": "yahoo_search", "label": "Yahoo Finance Search", "purpose": "Theme and ticker headline search", "endpoint_field": "yahoo_search_endpoint"},
            {"id": "finnhub_company_news", "label": "Finnhub Company News", "purpose": "Recent company headlines for leading theme tickers", "credential_env": ["FINNHUB_API_KEY"], "endpoint_field": "finnhub_company_news_endpoint"},
        ],
        "custom_sources": [],
        "enabled_sources": ["yahoo_search"],
        "articles_per_theme": 6,
        "query_ticker_count": 3,
        "finnhub_days_back": 7,
        "request_timeout_seconds": 12,
        "sentiment_word_weight": 8.0,
        "positive_keywords": sorted(POSITIVE_WORDS),
        "negative_keywords": sorted(NEGATIVE_WORDS),
        "yahoo_search_endpoint": "https://query1.finance.yahoo.com/v1/finance/search",
        "finnhub_company_news_endpoint": "https://finnhub.io/api/v1/company-news",
        "field_schema": {
            "custom_sources": _field("Custom news sources", "source_list", "Managed through Add Source.", hidden=True),
            "enabled_sources": _field("Enabled news sources", "list", "Comma-separated source IDs. Finnhub requires FINNHUB_API_KEY in .env.", choices=["yahoo_search", "finnhub_company_news"]),
            "articles_per_theme": _field("Articles retained per theme", "integer", "Maximum de-duplicated headlines retained and scored for each active theme.", min=1, max=50),
            "query_ticker_count": _field("Tickers included per query", "integer", "Number of leading theme tickers included in Yahoo search and queried individually on Finnhub.", min=1, max=10),
            "finnhub_days_back": _field("Finnhub history days", "integer", "Calendar days included in each Finnhub company-news request.", min=1, max=365),
            "request_timeout_seconds": _field("HTTP timeout (seconds)", "number", "Per-request timeout for news endpoints.", min=2, max=60),
            "sentiment_word_weight": _field("Sentiment points per keyword", "number", "Points added per positive token and removed per negative token from a neutral score of 50.", min=0, max=25),
            "positive_keywords": _field("Positive sentiment keywords", "list", "Comma-separated lowercase words counted as positive. Add or remove terms to tune headline scoring.", min_items=0, max_items=250),
            "negative_keywords": _field("Negative sentiment keywords", "list", "Comma-separated lowercase words counted as negative. Add or remove terms to tune headline scoring.", min_items=0, max_items=250),
            "yahoo_search_endpoint": _field("Yahoo news-search endpoint", "url", "HTTP(S) endpoint; q, newsCount, and quotesCount are sent as query parameters."),
            "finnhub_company_news_endpoint": _field("Finnhub company-news endpoint", "url", "HTTP(S) endpoint; symbol, from, to, and token are sent as query parameters."),
        },
    },
    "social": {
        "title": "Social: Reddit and Hacker News discussion",
        "description": "Searches the configured Reddit communities and Hacker News for each active theme, de-duplicates posts, and applies its own sentiment dictionary.",
        "source_inventory": [
            {"id": "reddit_rss", "label": "Reddit RSS", "purpose": "New posts in configured investing communities", "endpoint_field": "reddit_rss_endpoint"},
            {"id": "hackernews", "label": "Hacker News Algolia", "purpose": "Technology and market discussion search", "endpoint_field": "hackernews_endpoint"},
        ],
        "custom_sources": [],
        "enabled_sources": ["reddit_rss", "hackernews"],
        "subreddits": ["options", "stocks", "wallstreetbets"],
        "hackernews_terms": ["trading", "stocks", "market", "options", "ai"],
        "query_ticker_count": 2,
        "items_per_source": 5,
        "items_per_theme": 8,
        "reddit_sort": "new",
        "reddit_time_window": "week",
        "request_timeout_seconds": 12,
        "sentiment_word_weight": 8.0,
        "positive_keywords": sorted(POSITIVE_WORDS),
        "negative_keywords": sorted(NEGATIVE_WORDS),
        "reddit_rss_endpoint": "https://www.reddit.com/r/{subreddits}/search.rss",
        "hackernews_endpoint": "https://hn.algolia.com/api/v1/search",
        "field_schema": {
            "custom_sources": _field("Custom social sources", "source_list", "Managed through Add Source.", hidden=True),
            "enabled_sources": _field("Enabled social sources", "list", "Comma-separated source IDs: reddit_rss and/or hackernews.", choices=["reddit_rss", "hackernews"]),
            "subreddits": _field("Subreddits", "list", "Comma-separated subreddit names without r/. They are combined into one restricted RSS search.", min_items=0, max_items=50),
            "hackernews_terms": _field("Hacker News context terms", "list", "Extra terms appended to each active-theme query. Keep this short to avoid over-restricting results.", min_items=0, max_items=25),
            "query_ticker_count": _field("Tickers included per query", "integer", "Number of theme tickers appended to each social query.", min=0, max=10),
            "items_per_source": _field("Items requested per source", "integer", "Maximum Reddit entries and Hacker News hits read for each theme/source.", min=1, max=50),
            "items_per_theme": _field("Items retained per theme", "integer", "Maximum de-duplicated social items retained and scored per theme.", min=1, max=100),
            "reddit_sort": _field("Reddit sort", "text", "Reddit search sort such as new, relevance, top, or comments.", placeholder="new"),
            "reddit_time_window": _field("Reddit time window", "text", "Reddit t value such as hour, day, week, month, year, or all.", placeholder="week"),
            "request_timeout_seconds": _field("HTTP timeout (seconds)", "number", "Per-request timeout for social endpoints.", min=2, max=60),
            "sentiment_word_weight": _field("Sentiment points per keyword", "number", "Points added or removed per configured keyword from a neutral score of 50.", min=0, max=25),
            "positive_keywords": _field("Positive sentiment keywords", "list", "Comma-separated lowercase positive tokens.", min_items=0, max_items=250),
            "negative_keywords": _field("Negative sentiment keywords", "list", "Comma-separated lowercase negative tokens.", min_items=0, max_items=250),
            "reddit_rss_endpoint": _field("Reddit RSS endpoint", "url", "HTTP(S) URL template containing {subreddits}. Use {subreddits} for the plus-separated community list."),
            "hackernews_endpoint": _field("Hacker News endpoint", "url", "HTTP(S) Algolia search endpoint; query, tags, and hitsPerPage are query parameters."),
        },
    },
    "fundamentals": {
        "title": "Fundamentals: SEC XBRL company facts",
        "description": "Maps tickers to SEC CIKs, reads annual USD XBRL facts, calculates revenue and net-income YoY growth, and converts those values into a configurable 0–100 score.",
        "source_inventory": [
            {"id": "sec_companyfacts", "label": "SEC EDGAR Company Facts", "purpose": "Official CIK mapping and us-gaap annual facts", "endpoint_field": "sec_companyfacts_endpoint"},
        ],
        "custom_sources": [],
        "enabled_sources": ["sec_companyfacts"],
        "max_symbols": 14,
        "request_timeout_seconds": 12,
        "fiscal_period": "FY",
        "currency_unit": "USD",
        "minimum_history_years": 2,
        "minimum_revenue_yoy_pct": 10.0,
        "base_score": 50.0,
        "revenue_growth_weight": 1.0,
        "revenue_contribution_cap": 25.0,
        "net_income_growth_weight": 0.5,
        "net_income_contribution_cap": 20.0,
        "final_watchlist_weight": 0.13,
        "revenue_tags": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"],
        "net_income_tags": ["NetIncomeLoss"],
        "sec_ticker_map_endpoint": "https://www.sec.gov/files/company_tickers.json",
        "sec_companyfacts_endpoint": "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
        "field_schema": {
            "custom_sources": _field("Custom fundamental sources", "source_list", "Managed through Add Source.", hidden=True),
            "enabled_sources": _field("Enabled fundamental sources", "list", "The implemented source ID is sec_companyfacts. Empty disables the fundamental lookup.", choices=["sec_companyfacts"]),
            "max_symbols": _field("Maximum companies per scan", "integer", "Top preliminary candidates sent to SEC fundamentals and options deep checks.", min=1, max=100),
            "request_timeout_seconds": _field("HTTP timeout (seconds)", "number", "Per-request timeout for SEC ticker-map and company-facts calls.", min=2, max=60),
            "fiscal_period": _field("Fiscal period", "text", "SEC XBRL fp value used to select observations. FY means annual filings.", placeholder="FY"),
            "currency_unit": _field("XBRL currency unit", "text", "Unit key read from SEC facts, normally USD.", placeholder="USD"),
            "minimum_history_years": _field("Minimum annual history", "integer", "At least this many unique fiscal years are required to calculate YoY growth.", min=2, max=20),
            "minimum_revenue_yoy_pct": _field("Revenue-growth benchmark (%)", "number", "Shown as a pass/fail benchmark in each company result; it is not a hard exclusion filter.", min=-100, max=1000),
            "base_score": _field("Base fundamental score", "number", "Neutral starting score before revenue and income growth contributions.", min=0, max=100),
            "revenue_growth_weight": _field("Revenue growth weight", "number", "Revenue YoY percentage is multiplied by this value before clipping.", min=-10, max=10),
            "revenue_contribution_cap": _field("Revenue contribution cap", "number", "Maximum absolute points revenue growth can add or remove.", min=0, max=100),
            "net_income_growth_weight": _field("Net-income growth weight", "number", "Net-income YoY percentage is multiplied by this value before clipping.", min=-10, max=10),
            "net_income_contribution_cap": _field("Net-income contribution cap", "number", "Maximum absolute points net-income growth can add or remove.", min=0, max=100),
            "final_watchlist_weight": _field("Final-score fundamental weight", "number", "Weight assigned to the 0–100 fundamental score in final watchlist ranking.", min=0, max=1),
            "revenue_tags": _field("Revenue XBRL tags", "list", "Tags are tried in order; the first one with enough annual history is used.", min_items=1, max_items=50),
            "net_income_tags": _field("Net-income XBRL tags", "list", "Tags are tried in order; the first one with enough annual history is used.", min_items=1, max_items=50),
            "sec_ticker_map_endpoint": _field("SEC ticker-map endpoint", "url", "HTTP(S) JSON endpoint mapping stock tickers to CIK values."),
            "sec_companyfacts_endpoint": _field("SEC company-facts endpoint", "url", "HTTP(S) URL template containing {cik}; the code supplies a zero-padded 10-digit CIK."),
        },
    },
}


class SourcesConfigValidationError(ValueError):
    pass


def _validate_config_value(section_key, field_key, value, meta):
    field_type = meta.get("type", "text")
    label = f"{section_key}.{field_key}"
    if field_type == "object":
        if not isinstance(value, dict):
            raise SourcesConfigValidationError(f"{label} must be an object")
        return copy.deepcopy(value)
    if field_type == "source_list":
        if not isinstance(value, list):
            raise SourcesConfigValidationError(f"{label} must be a list of source definitions")
        if len(value) > 20:
            raise SourcesConfigValidationError(f"{label} supports at most 20 custom sources")
        built_in_ids = [source["id"] for source in DEFAULT_SOURCES_CONFIG[section_key]["source_inventory"]]
        validated = []
        seen = set()
        for source in value:
            try:
                item = validate_custom_source(section_key, source, built_in_ids)
            except CustomSourceValidationError as exc:
                raise SourcesConfigValidationError(str(exc)) from exc
            if item["id"] in seen:
                raise SourcesConfigValidationError(f"Duplicate custom source ID: {item['id']}")
            seen.add(item["id"])
            validated.append(item)
        return validated
    if field_type == "list":
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
            raise SourcesConfigValidationError(f"{label} must be a list of text values")
        value = [item.strip() for item in value if item.strip()]
        if len(value) < meta.get("min_items", 0) or len(value) > meta.get("max_items", 1000):
            raise SourcesConfigValidationError(f"{label} has an invalid number of entries")
        choices = meta.get("choices")
        if choices and any(item not in choices for item in value):
            raise SourcesConfigValidationError(f"{label} contains an unsupported source ID")
        return value
    if field_type in ("number", "integer"):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise SourcesConfigValidationError(f"{label} must be a finite number")
        value = int(value) if field_type == "integer" else float(value)
        if value < meta.get("min", value) or value > meta.get("max", value):
            raise SourcesConfigValidationError(f"{label} must be between {meta.get('min')} and {meta.get('max')}")
        return value
    if not isinstance(value, str):
        raise SourcesConfigValidationError(f"{label} must be text")
    value = value.strip()
    if field_type == "url":
        parsed = urllib.parse.urlparse(value)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise SourcesConfigValidationError(f"{label} must be a full HTTP(S) URL")
    return value


def _merge_sources_config(base_config, new_config):
    if not isinstance(new_config, dict):
        raise SourcesConfigValidationError("Configuration payload must be an object")
    merged = copy.deepcopy(base_config)
    for section_key, section_update in new_config.items():
        if section_key not in merged:
            raise SourcesConfigValidationError(f"Unknown configuration section: {section_key}")
        if not isinstance(section_update, dict):
            raise SourcesConfigValidationError(f"{section_key} must be an object")
        schema = merged[section_key]["field_schema"]
        for field_key, value in section_update.items():
            if field_key not in schema:
                raise SourcesConfigValidationError(f"Unknown configuration field: {section_key}.{field_key}")
            merged[section_key][field_key] = _validate_config_value(section_key, field_key, value, schema[field_key])
    discovery = merged["discovery"]
    if discovery["minimum_sectors_count"] > discovery["top_sectors_count"]:
        raise SourcesConfigValidationError("discovery.minimum_sectors_count cannot exceed top_sectors_count")
    market = merged["market"]
    if market["sma_short_days"] >= market["sma_long_days"]:
        raise SourcesConfigValidationError("market.sma_short_days must be lower than sma_long_days")
    required_templates = [
        ("discovery", "yahoo_chart_endpoint", "{symbol}"),
        ("market", "yahoo_chart_endpoint", "{symbol}"),
        ("market", "massive_endpoint", "{symbol}"),
        ("market", "cboe_endpoint", "{underlying}"),
        ("social", "reddit_rss_endpoint", "{subreddits}"),
        ("fundamentals", "sec_companyfacts_endpoint", "{cik}"),
    ]
    for section_key, field_key, token in required_templates:
        if token not in merged[section_key][field_key]:
            raise SourcesConfigValidationError(f"{section_key}.{field_key} must contain {token}")
    for entry in discovery["sector_etfs"]:
        ticker = entry.partition(":")[0].strip().upper()
        if not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", ticker):
            raise SourcesConfigValidationError(f"Invalid discovery ETF ticker: {ticker or entry}")
    market["benchmark_symbols"] = [symbol.upper() for symbol in market["benchmark_symbols"]]
    market["calibration_symbols"] = [symbol.upper() for symbol in market["calibration_symbols"]]
    for symbol in market["benchmark_symbols"] + market["calibration_symbols"]:
        if not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", symbol):
            raise SourcesConfigValidationError(f"Invalid market symbol: {symbol}")
    for subreddit in merged["social"]["subreddits"]:
        if not re.fullmatch(r"[A-Za-z0-9_]{1,50}", subreddit.replace("r/", "")):
            raise SourcesConfigValidationError(f"Invalid subreddit name: {subreddit}")
    for section_key in ("news", "social"):
        merged[section_key]["positive_keywords"] = [word.lower() for word in merged[section_key]["positive_keywords"]]
        merged[section_key]["negative_keywords"] = [word.lower() for word in merged[section_key]["negative_keywords"]]
    return merged


def _load_sources_config():
    try:
        with open(SOURCE_CONFIG_PATH, "r", encoding="utf-8") as handle:
            stored = json.load(handle)
        return _merge_sources_config(DEFAULT_SOURCES_CONFIG, stored)
    except FileNotFoundError:
        return copy.deepcopy(DEFAULT_SOURCES_CONFIG)
    except Exception:
        return copy.deepcopy(DEFAULT_SOURCES_CONFIG)


def _persist_sources_config(config):
    os.makedirs(os.path.dirname(SOURCE_CONFIG_PATH), exist_ok=True)
    values_only = {}
    for section_key, section in config.items():
        values_only[section_key] = {key: section[key] for key in section["field_schema"]}
    temp_path = f"{SOURCE_CONFIG_PATH}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(values_only, handle, indent=2, sort_keys=True)
    os.replace(temp_path, SOURCE_CONFIG_PATH)


def _credential_state(requirements):
    if not requirements:
        return "public"
    for requirement in requirements:
        alternatives = requirement.split("|")
        if not any(os.getenv(name, "") for name in alternatives):
            return "missing"
    return "configured"


ACTIVE_SOURCES_CONFIG = _load_sources_config()
SOURCES_CONFIG_UPDATED_AT = utc_now() if "utc_now" in globals() else datetime.now(timezone.utc).isoformat()


def get_sources_config():
    with SOURCE_CONFIG_LOCK:
        config = copy.deepcopy(ACTIVE_SOURCES_CONFIG)
    for section in config.values():
        enabled = set(section.get("enabled_sources", []))
        for source in section.get("source_inventory", []):
            source["enabled"] = source["id"] in enabled
            source["credential_status"] = _credential_state(source.get("credential_env"))
            endpoint_field = source.get("endpoint_field")
            if endpoint_field:
                source["endpoint"] = section.get(endpoint_field)
        for source in section.get("custom_sources", []):
            credential_env = source.get("credential_env")
            section["source_inventory"].append({
                "id": source["id"],
                "label": source["label"],
                "purpose": source.get("purpose"),
                "endpoint": source.get("endpoint"),
                "enabled": source.get("enabled", True),
                "credential_env": [credential_env] if credential_env else [],
                "credential_status": custom_credential_state([credential_env] if credential_env else []),
                "custom": True,
                "adapter": source.get("adapter"),
                "priority": source.get("priority"),
            })
    return config


def get_sources_config_meta():
    return {
        "updated_at": SOURCES_CONFIG_UPDATED_AT,
        "persistence": "runtime/sources_config.json",
        "apply_timing": "next scan or quote-router refresh",
    }


def update_sources_config(new_config):
    global ACTIVE_SOURCES_CONFIG, SOURCES_CONFIG_UPDATED_AT
    with SOURCE_CONFIG_LOCK:
        updated = _merge_sources_config(ACTIVE_SOURCES_CONFIG, new_config)
        _persist_sources_config(updated)
        ACTIVE_SOURCES_CONFIG = updated
        SOURCES_CONFIG_UPDATED_AT = datetime.now(timezone.utc).isoformat()
    return get_sources_config()


def reset_sources_config(section_key=None):
    global ACTIVE_SOURCES_CONFIG, SOURCES_CONFIG_UPDATED_AT
    with SOURCE_CONFIG_LOCK:
        if section_key is None:
            updated = copy.deepcopy(DEFAULT_SOURCES_CONFIG)
        elif section_key in DEFAULT_SOURCES_CONFIG:
            updated = copy.deepcopy(ACTIVE_SOURCES_CONFIG)
            updated[section_key] = copy.deepcopy(DEFAULT_SOURCES_CONFIG[section_key])
        else:
            raise SourcesConfigValidationError(f"Unknown configuration section: {section_key}")
        _persist_sources_config(updated)
        ACTIVE_SOURCES_CONFIG = updated
        SOURCES_CONFIG_UPDATED_AT = datetime.now(timezone.utc).isoformat()
    return get_sources_config()


def get_custom_source_contracts():
    return contracts_for_ui()


def upsert_custom_source(section_key, source_definition):
    if section_key not in ACTIVE_SOURCES_CONFIG:
        raise SourcesConfigValidationError(f"Unknown configuration section: {section_key}")
    existing = copy.deepcopy(ACTIVE_SOURCES_CONFIG[section_key].get("custom_sources", []))
    source_id = str((source_definition or {}).get("id") or "").strip().lower()
    replaced = False
    for index, source in enumerate(existing):
        if source.get("id") == source_id:
            existing[index] = source_definition
            replaced = True
            break
    if not replaced:
        existing.append(source_definition)
    return update_sources_config({section_key: {"custom_sources": existing}})


def remove_custom_source(section_key, source_id):
    if section_key not in ACTIVE_SOURCES_CONFIG:
        raise SourcesConfigValidationError(f"Unknown configuration section: {section_key}")
    existing = copy.deepcopy(ACTIVE_SOURCES_CONFIG[section_key].get("custom_sources", []))
    filtered = [source for source in existing if source.get("id") != source_id]
    if len(filtered) == len(existing):
        raise SourcesConfigValidationError(f"Custom source not found: {source_id}")
    return update_sources_config({section_key: {"custom_sources": filtered}})


def test_custom_source(section_key, source_definition, context=None):
    if section_key not in ACTIVE_SOURCES_CONFIG:
        raise SourcesConfigValidationError(f"Unknown configuration section: {section_key}")
    built_in_ids = [source["id"] for source in DEFAULT_SOURCES_CONFIG[section_key]["source_inventory"]]
    try:
        validated = validate_custom_source(section_key, source_definition, built_in_ids)
        return GenericJsonSource(validated).test(context)
    except (CustomSourceValidationError, CustomSourceRequestError) as exc:
        raise SourcesConfigValidationError(str(exc)) from exc

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


def sentiment_score(texts, positive_words=None, negative_words=None, word_weight=8.0):
    joined = " ".join(t or "" for t in texts).lower()
    tokens = re.findall(r"[a-zA-Z][a-zA-Z\-']+", joined)
    if not tokens:
        return 50
    positive_set = set(positive_words if positive_words is not None else POSITIVE_WORDS)
    negative_set = set(negative_words if negative_words is not None else NEGATIVE_WORDS)
    positives = sum(1 for token in tokens if token in positive_set)
    negatives = sum(1 for token in tokens if token in negative_set)
    return clamp(50 + (positives - negatives) * float(word_weight))


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
    def fetch_bars(
        self,
        symbol,
        range_value="6mo",
        interval="1d",
        endpoint="https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
        timeout=12,
    ):
        encoded = urllib.parse.quote(symbol)
        url = endpoint.replace("{symbol}", encoded)
        response = requests.get(
            url,
            params={"range": range_value, "interval": interval},
            headers=HTTP_HEADERS,
            timeout=float(timeout),
        )
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


def fetch_configured_bars(yahoo, symbol, config, range_value, interval):
    candidates = []
    if "yahoo_chart" in config.get("enabled_sources", []):
        candidates.append((100, "yahoo_chart", None))
    for source in config.get("custom_sources", []):
        if source.get("enabled") and source.get("adapter") == "bars_json":
            candidates.append((source.get("priority", 50), source["id"], source))
    errors = []
    for _, source_id, source in sorted(candidates, key=lambda item: (item[0], item[1])):
        try:
            if source is None:
                bars = yahoo.fetch_bars(
                    symbol,
                    range_value=range_value,
                    interval=interval,
                    endpoint=config["yahoo_chart_endpoint"],
                    timeout=config["request_timeout_seconds"],
                )
            else:
                bars = GenericJsonSource(source).fetch_bars(symbol)
            return bars, source_id
        except Exception as exc:
            errors.append(f"{source_id}: {str(exc)[:160]}")
    if not candidates:
        raise RuntimeError("No historical bar source is enabled")
    raise RuntimeError("; ".join(errors) or "All historical bar sources failed")


def _bounded_calibration_value(section_key, field_key, value):
    meta = DEFAULT_SOURCES_CONFIG[section_key]["field_schema"][field_key]
    return round(max(meta.get("min", value), min(meta.get("max", value), value)), 6)


def calibrate_source_scoring(section_key):
    if section_key not in ("discovery", "market"):
        raise SourcesConfigValidationError("Calibration is supported only for discovery and market scoring")
    config = get_sources_config()[section_key]
    if section_key == "discovery":
        symbols = list(dict.fromkeys(
            entry.partition(":")[0].strip().upper()
            for entry in config["sector_etfs"]
            if entry.partition(":")[0].strip()
        ))
    else:
        symbols = list(dict.fromkeys(config["calibration_symbols"]))

    bars_by_symbol = {}
    sources_by_symbol = {}
    errors = {}
    yahoo = YahooChartProvider()
    max_workers = min(12, max(1, len(symbols)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                fetch_configured_bars,
                yahoo,
                symbol,
                config,
                config["calibration_lookback_range"],
                "1d",
            ): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                bars, source_id = future.result()
                bars_by_symbol[symbol] = bars
                sources_by_symbol[symbol] = source_id
            except Exception as exc:
                errors[symbol] = str(exc)[:200]
    minimum_symbols = 4
    if len(bars_by_symbol) < minimum_symbols:
        raise SourcesConfigValidationError(
            f"Calibration needs at least {minimum_symbols} symbols with sufficient daily history; "
            f"received {len(bars_by_symbol)}"
        )
    try:
        result = calibrate_scoring_weights(
            section_key,
            bars_by_symbol,
            config,
            target_days=config["calibration_target_days"],
            ridge=config["calibration_ridge_penalty"],
        )
    except CalibrationError as exc:
        raise SourcesConfigValidationError(str(exc)) from exc

    result["requested_symbols"] = symbols
    result["sources_by_symbol"] = sources_by_symbol
    result["fetch_errors"] = errors
    metrics = result["metrics"]
    passed_holdout = (
        metrics["validation_correlation"] > 0
        and metrics["validation_correlation_t_stat"] >= 1.96
        and metrics["validation_top_quartile_mean_return_pct"]
        > metrics["validation_all_mean_return_pct"]
    )
    if not passed_holdout:
        result["status"] = "rejected"
        result["limitations"].insert(
            0,
            "Holdout validation did not show statistically credible positive correlation (t ≥ 1.96) plus top-quartile lift, so fitted weights were not applied.",
        )
        update_sources_config({section_key: {"scoring_provenance": result}})
        return {"applied": False, "calibration": result, "config": get_sources_config()}

    fitted = result["weights"]
    if section_key == "discovery":
        weight_update = {
            "weight_5d_return": _bounded_calibration_value(section_key, "weight_5d_return", fitted["return_5d"]),
            "weight_20d_return": _bounded_calibration_value(section_key, "weight_20d_return", fitted["return_20d"]),
            "weight_volume_expansion": _bounded_calibration_value(section_key, "weight_volume_expansion", fitted["volume_expansion"]),
        }
    else:
        weight_update = {
            "weight_5d_return": _bounded_calibration_value(section_key, "weight_5d_return", fitted["return_5d"]),
            "weight_20d_return": _bounded_calibration_value(section_key, "weight_20d_return", fitted["return_20d"]),
            "weight_60d_return": _bounded_calibration_value(section_key, "weight_60d_return", fitted["return_60d"]),
            "sma_short_bonus": _bounded_calibration_value(section_key, "sma_short_bonus", fitted["above_short_sma"]),
            "sma_long_bonus": _bounded_calibration_value(section_key, "sma_long_bonus", fitted["above_long_sma"]),
            "weight_volume_expansion": _bounded_calibration_value(section_key, "weight_volume_expansion", fitted["volume_expansion"]),
            "overbought_penalty": _bounded_calibration_value(section_key, "overbought_penalty", max(0, -fitted["overbought"])),
        }
    result["applied_config_weights"] = weight_update
    updated = update_sources_config({section_key: {**weight_update, "scoring_provenance": result}})
    return {"applied": True, "calibration": result, "config": updated}


class MarketDataAgent:
    def __init__(self):
        self.yahoo = YahooChartProvider()

    def run(self, symbols):
        config = get_sources_config()["market"]
        custom_bars = [
            source for source in config.get("custom_sources", [])
            if source.get("enabled") and source.get("adapter") == "bars_json"
        ]
        if "yahoo_chart" not in config["enabled_sources"] and not custom_bars:
            return {
                "summary": "Yahoo technical bars disabled in market configuration",
                "source": "none",
                "sources_used": [],
                "metrics": {},
                "errors": {},
                "status": "disabled",
            }
        metrics = {}
        errors = {}
        with ThreadPoolExecutor(max_workers=config["max_parallel_requests"]) as executor:
            futures = {
                executor.submit(self._symbol_metrics, symbol, config): symbol
                for symbol in sorted(set(symbols))
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    metrics[symbol] = future.result()
                except Exception as exc:
                    errors[symbol] = str(exc)
        sources_used = sorted({metric.get("source_id") for metric in metrics.values() if metric.get("source_id")})
        return {
            "summary": f"Historical bar metrics for {len(metrics)}/{len(set(symbols))} symbols",
            "source": " + ".join(sources_used) if sources_used else "none",
            "sources_used": sources_used,
            "metrics": metrics,
            "errors": errors,
            "status": "degraded" if errors else "ok"
        }

    def _symbol_metrics(self, symbol, config):
        bars, source_id = fetch_configured_bars(
            self.yahoo,
            symbol,
            config,
            config["chart_lookback_range"],
            config["chart_interval"],
        )
        closes = [bar["close"] for bar in bars]
        volumes = [bar["volume"] for bar in bars]
        close = closes[-1]
        short_days = config["sma_short_days"]
        long_days = config["sma_long_days"]
        sma_short = average(closes[-short_days:])
        sma_long = average(closes[-long_days:]) if len(closes) >= long_days else average(closes)
        avg_volume = average(volumes[-short_days - 1:-1]) or average(volumes[-short_days:])
        volume_ratio = volumes[-1] / avg_volume if avg_volume else None
        ret5 = pct_change(close, closes[-6]) if len(closes) >= 6 else None
        ret20 = pct_change(close, closes[-21]) if len(closes) >= 21 else None
        ret60 = pct_change(close, closes[-61]) if len(closes) >= 61 else None
        rsi_value = rsi(closes, config["rsi_days"])
        trend_score = 50
        for ret, weight in [
            (ret5, config["weight_5d_return"]),
            (ret20, config["weight_20d_return"]),
            (ret60, config["weight_60d_return"]),
        ]:
            if ret is not None:
                trend_score += ret * weight
        if sma_short and close > sma_short:
            trend_score += config["sma_short_bonus"]
        if sma_long and close > sma_long:
            trend_score += config["sma_long_bonus"]
        if volume_ratio and volume_ratio > config["minimum_volume_ratio"]:
            trend_score += min(
                config["volume_bonus_cap"],
                (volume_ratio - config["minimum_volume_ratio"]) * config["weight_volume_expansion"],
            )
        if rsi_value and rsi_value > config["overbought_rsi"]:
            trend_score -= config["overbought_penalty"]
        return {
            "source_id": source_id,
            "price": safe_round(close),
            "last_date": bars[-1]["date"],
            "return_5d": safe_round(ret5),
            "return_20d": safe_round(ret20),
            "return_60d": safe_round(ret60),
            "sma20": safe_round(sma_short),
            "sma50": safe_round(sma_long),
            "sma_short": safe_round(sma_short),
            "sma_long": safe_round(sma_long),
            "sma_short_days": short_days,
            "sma_long_days": long_days,
            "above_sma20": bool(sma_short and close > sma_short),
            "above_sma50": bool(sma_long and close > sma_long),
            "above_sma_short": bool(sma_short and close > sma_short),
            "above_sma_long": bool(sma_long and close > sma_long),
            "volume_ratio": safe_round(volume_ratio),
            "rsi14": safe_round(rsi_value),
            "rsi": safe_round(rsi_value),
            "rsi_days": config["rsi_days"],
            "trend_score": int(clamp(trend_score)),
        }


class AlpacaDiscoveryAgent:
    def __init__(self):
        self.api_key = os.getenv("ALPACA_API_KEY", "")
        self.secret_key = os.getenv("ALPACA_SECRET_KEY", "")

    def run(self):
        config = get_sources_config()["discovery"]
        if "alpaca_screener" not in config["enabled_sources"]:
            return {
                "summary": "Alpaca screeners disabled in discovery configuration",
                "status": "disabled",
                "sources_used": [],
                "movers": [],
                "actives": [],
            }
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
            movers = screener.get_market_movers(MarketMoversRequest(top=config["alpaca_movers_count"], market_type=MarketType.STOCKS))
            actives = screener.get_most_actives(MostActivesRequest(top=config["alpaca_actives_count"], by=MostActivesBy.VOLUME))
            return {
                "summary": "Alpaca screeners available",
                "status": "ok",
                "sources_used": ["alpaca_screener"],
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
        config = get_sources_config()["discovery"]
        configured_etfs = self._configured_etfs(config["sector_etfs"])
        has_bar_source = "yahoo_chart" in config["enabled_sources"] or any(
            source.get("enabled") and source.get("adapter") == "bars_json"
            for source in config.get("custom_sources", [])
        )
        etf_rankings = self._scan_sector_etfs(configured_etfs, config) if has_bar_source else []
        active_sectors = self._rank_sectors(etf_rankings, config)
        active_themes = self._build_active_themes(active_sectors)
        alpaca_extras = self._alpaca_mover_extras(
            alpaca_discovery,
            active_themes,
            config["alpaca_extra_limit"],
        )
        custom_extras, custom_screener_sources = self._custom_screener_extras(config)
        discovery_extras = []
        for symbol in alpaca_extras + custom_extras:
            if symbol not in discovery_extras:
                discovery_extras.append(symbol)
        discovery_extras = discovery_extras[:config["alpaca_extra_limit"]]
        if discovery_extras:
            active_themes.append({
                "name": "Dynamic screeners",
                "tickers": discovery_extras,
                "etfs": [],
                "keywords": ["market movers", "high volume", "momentum"],
            })
        bar_sources = sorted({ranking.get("source_id") for ranking in etf_rankings if ranking.get("source_id")})
        sources_used = bar_sources + list((alpaca_discovery or {}).get("sources_used", [])) + custom_screener_sources
        return {
            "summary": f"Discovered {len(active_themes)} active themes from {len(active_sectors)} sectors",
            "status": "ok" if etf_rankings or discovery_extras else "degraded",
            "sources_used": list(dict.fromkeys(sources_used)),
            "active_themes": active_themes,
            "alpaca_extras": discovery_extras,
            "discovery_metadata": {
                "etfs_configured": len(configured_etfs),
                "sectors_scanned": len(etf_rankings),
                "sectors_active": len(active_sectors),
                "active_sector_names": [s["sector"] for s in active_sectors],
                "etf_rankings": etf_rankings[:10],
            },
        }

    def _configured_etfs(self, entries):
        configured = []
        seen = set()
        static_sectors = {item["etf"]: item["sector"] for item in SECTOR_ETFS}
        for entry in entries:
            ticker, separator, sector = entry.partition(":")
            ticker = ticker.strip().upper()
            sector = sector.strip() if separator else static_sectors.get(ticker, "Custom")
            if ticker and ticker not in seen:
                configured.append({"etf": ticker, "sector": sector or "Custom"})
                seen.add(ticker)
        return configured

    def _scan_sector_etfs(self, configured_etfs, config):
        rankings = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(self._etf_momentum, item["etf"], config): item
                for item in configured_etfs
            }
            for future in as_completed(futures):
                item = futures[future]
                try:
                    metrics = future.result()
                    rankings.append({"etf": item["etf"], "sector": item["sector"], **metrics})
                except Exception:
                    pass
        return sorted(rankings, key=lambda x: x.get("momentum_score", 0), reverse=True)

    def _etf_momentum(self, etf, config):
        bars, source_id = fetch_configured_bars(
            self.yahoo,
            etf,
            config,
            config["benchmark_lookback_range"],
            config["chart_interval"],
        )
        closes = [b["close"] for b in bars]
        volumes = [b["volume"] for b in bars]
        ret5 = pct_change(closes[-1], closes[-6]) if len(closes) >= 6 else 0
        ret20 = pct_change(closes[-1], closes[-21]) if len(closes) >= 21 else 0
        avg_vol = average(volumes[-21:-1]) or average(volumes)
        vol_ratio = volumes[-1] / avg_vol if avg_vol else 1
        momentum = (
            (ret5 or 0) * config["weight_5d_return"]
            + (ret20 or 0) * config["weight_20d_return"]
            + max(0, (vol_ratio - 1)) * config["weight_volume_expansion"]
        )
        return {
            "source_id": source_id,
            "return_5d": safe_round(ret5),
            "return_20d": safe_round(ret20),
            "volume_ratio": safe_round(vol_ratio),
            "momentum_score": safe_round(momentum),
        }

    def _rank_sectors(self, etf_rankings, config):
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
            if s["score"] > config["minimum_momentum_score"] or len(active) < config["minimum_sectors_count"]:
                active.append(s)
            if len(active) >= config["top_sectors_count"]:
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

    def _alpaca_mover_extras(self, alpaca_discovery, active_themes, limit):
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
        return extras[:limit]

    def _custom_screener_extras(self, config):
        symbols = []
        sources_used = []
        for source in sorted(config.get("custom_sources", []), key=lambda item: item.get("priority", 50)):
            if not source.get("enabled") or source.get("adapter") != "screener_json":
                continue
            try:
                rows = GenericJsonSource(source).fetch_symbols()
                sources_used.append(source["id"])
                for row in rows:
                    if row["symbol"] not in symbols:
                        symbols.append(row["symbol"])
            except Exception:
                continue
        return symbols, sources_used


class NewsAgent:
    def run(self, themes):
        config = get_sources_config()["news"]
        enabled_sources = config["enabled_sources"]
        custom_sources = [
            source for source in config.get("custom_sources", [])
            if source.get("enabled") and source.get("adapter") == "items_json"
        ]
        custom_sources.sort(key=lambda item: item.get("priority", 50))
        sources_used = list(enabled_sources)
        items_by_theme = {}
        all_titles = []
        errors = {}
        for theme in themes:
            query = f"{theme['name']} stocks " + " ".join(theme["tickers"][:config["query_ticker_count"]])
            items = []
            if "yahoo_search" in enabled_sources:
                try:
                    items.extend(self._yahoo_news(query, config))
                except Exception as exc:
                    errors[f"{theme['name']}:yahoo_search"] = str(exc)[:220]
            if "finnhub_company_news" in enabled_sources:
                try:
                    items.extend(self._finnhub_news(theme, config))
                except Exception as exc:
                    errors[f"{theme['name']}:finnhub_company_news"] = str(exc)[:220]
            for source in custom_sources:
                try:
                    custom_items = GenericJsonSource(source).fetch_items(
                        query,
                        theme["name"],
                        theme["tickers"][0] if theme.get("tickers") else "",
                    )
                    items.extend(custom_items)
                    if source["id"] not in sources_used:
                        sources_used.append(source["id"])
                except Exception as exc:
                    errors[f"{theme['name']}:{source['id']}"] = str(exc)[:220]
            deduped = []
            seen = set()
            for item in items:
                key = (item.get("title") or "").strip().lower()
                if key and key not in seen:
                    seen.add(key)
                    deduped.append(item)
            selected = deduped[:config["articles_per_theme"]]
            items_by_theme[theme["name"]] = selected
            all_titles.extend(item["title"] for item in selected)
        status = "disabled" if not enabled_sources and not custom_sources else "degraded" if errors else "ok"
        return {
            "summary": f"News sources returned {sum(len(v) for v in items_by_theme.values())} articles",
            "source": " + ".join(sources_used) if sources_used else "none",
            "sources_used": sources_used,
            "items_by_theme": items_by_theme,
            "sentiment": sentiment_score(
                all_titles,
                config["positive_keywords"],
                config["negative_keywords"],
                config["sentiment_word_weight"],
            ),
            "errors": errors,
            "status": status,
        }

    def _yahoo_news(self, query, config):
        response = requests.get(
            config["yahoo_search_endpoint"],
            params={"q": query, "newsCount": config["articles_per_theme"], "quotesCount": 0},
            headers=HTTP_HEADERS,
            timeout=config["request_timeout_seconds"],
        )
        response.raise_for_status()
        raw_items = response.json().get("news") or []
        items = []
        for item in raw_items[:config["articles_per_theme"]]:
            title = item.get("title") or ""
            if not title:
                continue
            items.append({
                "title": title,
                "publisher": item.get("publisher"),
                "link": item.get("link"),
                "published_at": item.get("providerPublishTime"),
                "source": "Yahoo Finance",
            })
        return items

    def _finnhub_news(self, theme, config):
        api_key = os.getenv("FINNHUB_API_KEY", "")
        if not api_key:
            raise RuntimeError("FINNHUB_API_KEY is not configured")
        end_date = date.today()
        start_date = end_date - timedelta(days=config["finnhub_days_back"])
        items = []
        for symbol in theme["tickers"][:config["query_ticker_count"]]:
            response = requests.get(
                config["finnhub_company_news_endpoint"],
                params={
                    "symbol": symbol,
                    "from": start_date.isoformat(),
                    "to": end_date.isoformat(),
                    "token": api_key,
                },
                headers=HTTP_HEADERS,
                timeout=config["request_timeout_seconds"],
            )
            response.raise_for_status()
            for item in (response.json() or [])[:config["articles_per_theme"]]:
                title = item.get("headline") or ""
                if title:
                    items.append({
                        "title": title,
                        "publisher": item.get("source"),
                        "link": item.get("url"),
                        "published_at": item.get("datetime"),
                        "source": "Finnhub",
                        "symbol": symbol,
                    })
        return items


class SocialSentimentAgent:
    def run(self, themes):
        config = get_sources_config()["social"]
        enabled_sources = config["enabled_sources"]
        custom_sources = [
            source for source in config.get("custom_sources", [])
            if source.get("enabled") and source.get("adapter") == "items_json"
        ]
        custom_sources.sort(key=lambda item: item.get("priority", 50))
        sources_used = list(enabled_sources)
        items_by_theme = {}
        all_titles = []
        errors = {}
        for theme in themes:
            query = f"{theme['name']} " + " ".join(theme["tickers"][:config["query_ticker_count"]])
            items = []
            fetchers = []
            if "reddit_rss" in enabled_sources:
                fetchers.append(("reddit_rss", self._reddit_rss))
            if "hackernews" in enabled_sources:
                fetchers.append(("hackernews", self._hacker_news))
            for source_id, fetcher in fetchers:
                try:
                    items.extend(fetcher(query, config))
                except Exception as exc:
                    errors[f"{theme['name']}:{source_id}"] = str(exc)[:180]
            for source in custom_sources:
                try:
                    items.extend(GenericJsonSource(source).fetch_items(
                        query,
                        theme["name"],
                        theme["tickers"][0] if theme.get("tickers") else "",
                    ))
                    if source["id"] not in sources_used:
                        sources_used.append(source["id"])
                except Exception as exc:
                    errors[f"{theme['name']}:{source['id']}"] = str(exc)[:180]
            deduped = []
            seen = set()
            for item in items:
                key = item["title"].lower()
                if key not in seen:
                    seen.add(key)
                    deduped.append(item)
            selected = deduped[:config["items_per_theme"]]
            items_by_theme[theme["name"]] = selected
            all_titles.extend(item["title"] for item in selected)
        status = "disabled" if not enabled_sources and not custom_sources else "degraded" if errors else "ok"
        return {
            "summary": f"Social/news-discussion proxy returned {sum(len(v) for v in items_by_theme.values())} posts",
            "source": " + ".join(sources_used) if sources_used else "none",
            "sources_used": sources_used,
            "items_by_theme": items_by_theme,
            "sentiment": sentiment_score(
                all_titles,
                config["positive_keywords"],
                config["negative_keywords"],
                config["sentiment_word_weight"],
            ),
            "errors": errors,
            "status": status,
        }

    def _reddit_rss(self, query, config):
        subreddit_path = "+".join(
            urllib.parse.quote(item.strip().replace("r/", ""))
            for item in config["subreddits"]
            if item.strip()
        ) or "all"
        url = config["reddit_rss_endpoint"].replace("{subreddits}", subreddit_path)
        response = requests.get(
            url,
            params={
                "q": query,
                "sort": config["reddit_sort"],
                "t": config["reddit_time_window"],
                "restrict_sr": "on" if subreddit_path != "all" else "off",
            },
            headers=HTTP_HEADERS,
            timeout=config["request_timeout_seconds"],
        )
        response.raise_for_status()
        root = ET.fromstring(response.text)
        namespace = {"atom": "http://www.w3.org/2005/Atom"}
        items = []
        for entry in root.findall("atom:entry", namespace)[:config["items_per_source"]]:
            title = entry.findtext("atom:title", default="", namespaces=namespace)
            link_el = entry.find("atom:link", namespace)
            link = link_el.attrib.get("href") if link_el is not None else None
            if title:
                items.append({"title": title, "source": "Reddit", "link": link})
        return items

    def _hacker_news(self, query, config):
        terms = " ".join(config["hackernews_terms"][:3])
        full_query = f"{query} {terms}".strip()
        response = requests.get(
            config["hackernews_endpoint"],
            params={"query": full_query, "tags": "story", "hitsPerPage": config["items_per_source"]},
            headers=HTTP_HEADERS,
            timeout=config["request_timeout_seconds"],
        )
        response.raise_for_status()
        items = []
        for hit in response.json().get("hits", [])[:config["items_per_source"]]:
            title = hit.get("title") or hit.get("story_title")
            if title:
                items.append({"title": title, "source": "Hacker News", "link": hit.get("url")})
        return items


class FundamentalsAgent:
    def __init__(self):
        self._ticker_map = None

    def run(self, symbols):
        config = get_sources_config()["fundamentals"]
        custom_sources = [
            source for source in config.get("custom_sources", [])
            if source.get("enabled") and source.get("adapter") == "fundamentals_json"
        ]
        if "sec_companyfacts" not in config["enabled_sources"] and not custom_sources:
            return {
                "summary": "SEC fundamentals disabled in configuration",
                "source": "none",
                "sources_used": [],
                "fundamentals": {},
                "errors": {},
                "status": "disabled",
            }
        fundamentals = {}
        errors = {}
        sources_used = set()
        for symbol in symbols:
            try:
                fundamentals[symbol] = self._fundamentals_for_symbol(symbol, config)
                if fundamentals[symbol].get("source_id"):
                    sources_used.add(fundamentals[symbol]["source_id"])
            except Exception as exc:
                errors[symbol] = str(exc)[:220]
        return {
            "summary": f"Fundamental snapshots for {len(fundamentals)}/{len(symbols)} symbols",
            "source": " + ".join(sorted(sources_used)) if sources_used else "none",
            "sources_used": sorted(sources_used),
            "fundamentals": fundamentals,
            "errors": errors,
            "status": "degraded" if errors else "ok"
        }

    def _fundamentals_for_symbol(self, symbol, config):
        candidates = []
        if "sec_companyfacts" in config["enabled_sources"]:
            candidates.append((100, "sec_companyfacts", None))
        for source in config.get("custom_sources", []):
            if source.get("enabled") and source.get("adapter") == "fundamentals_json":
                candidates.append((source.get("priority", 50), source["id"], source))
        errors = []
        for _, source_id, source in sorted(candidates, key=lambda item: (item[0], item[1])):
            try:
                if source is None:
                    return self._sec_fundamentals(symbol, config)
                result = GenericJsonSource(source).fetch_fundamentals(symbol)
                return self._score_growth_result(result, config, source_id)
            except Exception as exc:
                errors.append(f"{source_id}: {str(exc)[:160]}")
        raise RuntimeError("; ".join(errors) or "No fundamental source is enabled")

    def _load_ticker_map(self, config):
        if self._ticker_map is not None:
            return self._ticker_map
        response = requests.get(
            config["sec_ticker_map_endpoint"],
            headers=HTTP_HEADERS,
            timeout=config["request_timeout_seconds"],
        )
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

    def _sec_fundamentals(self, symbol, config):
        ticker_map = self._load_ticker_map(config)
        if symbol not in ticker_map:
            raise RuntimeError("No SEC CIK mapping")
        company = ticker_map[symbol]
        url = config["sec_companyfacts_endpoint"].replace("{cik}", company["cik"])
        response = requests.get(
            url,
            headers=HTTP_HEADERS,
            timeout=config["request_timeout_seconds"],
        )
        response.raise_for_status()
        facts = response.json().get("facts", {}).get("us-gaap", {})
        revenue_growth = self._yoy_growth(facts, config["revenue_tags"], config)
        net_income_growth = self._yoy_growth(facts, config["net_income_tags"], config)
        return self._score_growth_result({
            "company": company["name"],
            "cik": company["cik"],
            "revenue_yoy": revenue_growth,
            "net_income_yoy": net_income_growth,
        }, config, "sec_companyfacts")

    def _score_growth_result(self, result, config, source_id):
        revenue_growth = result.get("revenue_yoy")
        net_income_growth = result.get("net_income_yoy")
        score = config["base_score"]
        if revenue_growth is not None:
            contribution = revenue_growth * config["revenue_growth_weight"]
            score += clamp(contribution, -config["revenue_contribution_cap"], config["revenue_contribution_cap"])
        if net_income_growth is not None:
            contribution = net_income_growth * config["net_income_growth_weight"]
            score += clamp(contribution, -config["net_income_contribution_cap"], config["net_income_contribution_cap"])
        return {
            "company": result.get("company"),
            "cik": result.get("cik"),
            "revenue_yoy": safe_round(revenue_growth),
            "net_income_yoy": safe_round(net_income_growth),
            "minimum_revenue_yoy_pct": config["minimum_revenue_yoy_pct"],
            "meets_revenue_growth_benchmark": bool(
                revenue_growth is not None and revenue_growth >= config["minimum_revenue_yoy_pct"]
            ),
            "fundamental_score": int(clamp(score)),
            "source_id": source_id,
        }

    def _yoy_growth(self, facts, tags, config):
        for tag in tags:
            units = facts.get(tag, {}).get("units", {})
            values = units.get(config["currency_unit"]) or []
            annual = [
                item for item in values
                if item.get("fp") == config["fiscal_period"] and item.get("fy") and item.get("val") is not None
            ]
            annual.sort(key=lambda item: (item.get("fy") or 0, item.get("filed") or ""))
            by_year = {}
            for item in annual:
                by_year[item["fy"]] = item["val"]
            if len(by_year) >= config["minimum_history_years"]:
                years = sorted(by_year)
                latest = by_year[years[-1]]
                previous = by_year[years[-2]]
                return pct_change(latest, previous)
        return None


class OptionsValidationAgent:
    def run(self, candidates, market_metrics):
        config = get_sources_config()["market"]
        if "cboe" not in config["enabled_sources"]:
            return {
                "summary": "Cboe option validation disabled in market configuration",
                "source": "none",
                "validations": {},
                "errors": {},
                "status": "disabled",
            }
        validations = {}
        errors = {}
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {
                executor.submit(
                    self._validate_symbol,
                    item["ticker"],
                    item["bias"],
                    market_metrics.get(item["ticker"], {}),
                    config,
                ): item
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

    def _validate_symbol(self, symbol, bias, market_metric, config):
        url = config["cboe_endpoint"].replace("{underlying}", urllib.parse.quote(symbol))
        response = requests.get(
            url,
            headers=HTTP_HEADERS,
            timeout=config["request_timeout_seconds"],
        )
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


class UnusualOptionsFlowAgent:
    """Scans CBOE delayed option chains for unusual volume relative to open interest.

    A Vol/OI ratio >= 3.0 on a single contract with meaningful premium is one of
    the most reliable leading indicators of informed positioning.  This agent
    scans every contract in the chain (both calls and puts), ranks anomalies,
    and returns a per-symbol flow digest that downstream scoring can use.
    """

    def run(self, symbols, market_metrics):
        config = get_sources_config()["market"]
        if "cboe" not in config["enabled_sources"]:
            return {
                "summary": "Unusual options flow scan disabled (cboe not in enabled_sources)",
                "status": "disabled",
                "flow_signals": {},
                "errors": {},
            }
        flow_config = config.get("unusual_flow", {})
        min_vol_oi = float(flow_config.get("min_vol_oi_ratio", 3.0))
        min_premium = float(flow_config.get("min_premium_dollars", 10000))
        min_oi = int(flow_config.get("min_open_interest", 50))
        max_dte = int(flow_config.get("max_dte", 90))

        flow_signals = {}
        errors = {}
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {
                executor.submit(
                    self._scan_symbol, symbol, market_metrics.get(symbol, {}),
                    config, min_vol_oi, min_premium, min_oi, max_dte,
                ): symbol
                for symbol in sorted(set(symbols))
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    result = future.result()
                    if result:
                        flow_signals[symbol] = result
                except Exception as exc:
                    errors[symbol] = str(exc)[:200]

        flagged = sum(1 for sig in flow_signals.values() if sig["has_unusual_activity"])
        return {
            "summary": f"Unusual options flow: {flagged} of {len(flow_signals)} symbols flagged",
            "source": "Cboe delayed options (Vol/OI scan)",
            "status": "degraded" if errors else "ok",
            "flow_signals": flow_signals,
            "errors": errors,
        }

    def _scan_symbol(self, symbol, market_metric, config, min_vol_oi, min_premium, min_oi, max_dte):
        url = config["cboe_endpoint"].replace("{underlying}", urllib.parse.quote(symbol))
        response = requests.get(url, headers=HTTP_HEADERS, timeout=config["request_timeout_seconds"])
        response.raise_for_status()
        payload = response.json()
        options = payload.get("data", {}).get("options") or []
        current_price = market_metric.get("price") or payload.get("data", {}).get("current_price")
        today = date.today()
        unusual = []
        total_call_vol = 0
        total_put_vol = 0
        total_call_oi = 0
        total_put_oi = 0

        for option in options:
            symbol_str = option.get("option", "")
            parsed = self._parse_occ(symbol_str)
            if not parsed:
                continue
            dte = (parsed["expiration"] - today).days
            if dte < 1 or dte > max_dte:
                continue

            volume = float(option.get("volume") or 0)
            oi = float(option.get("open_interest") or 0)
            bid = float(option.get("bid") or 0)
            ask = float(option.get("ask") or 0)
            mid = (bid + ask) / 2 if bid and ask else float(option.get("last_trade_price") or 0)

            side = "call" if parsed["type"] == "C" else "put"
            if side == "call":
                total_call_vol += volume
                total_call_oi += oi
            else:
                total_put_vol += volume
                total_put_oi += oi

            if oi < min_oi or volume < 1:
                continue

            vol_oi = volume / oi
            premium_total = mid * volume * 100
            if vol_oi < min_vol_oi or premium_total < min_premium:
                continue

            delta = option.get("delta")
            iv = option.get("iv")
            unusual.append({
                "contract": symbol_str,
                "side": side,
                "strike": parsed["strike"],
                "expiration": parsed["expiration"].isoformat(),
                "dte": dte,
                "volume": int(volume),
                "open_interest": int(oi),
                "vol_oi_ratio": safe_round(vol_oi),
                "mid": safe_round(mid),
                "premium_total": int(premium_total),
                "bid": safe_round(bid),
                "ask": safe_round(ask),
                "iv": safe_round(iv),
                "delta": safe_round(delta),
            })

        unusual.sort(key=lambda c: c["premium_total"], reverse=True)
        top_contracts = unusual[:5]

        total_vol = total_call_vol + total_put_vol
        put_call_ratio = safe_round(total_put_vol / total_call_vol) if total_call_vol > 0 else None
        call_pct = safe_round((total_call_vol / total_vol) * 100) if total_vol > 0 else None

        bullish = sum(1 for c in top_contracts if c["side"] == "call")
        bearish = sum(1 for c in top_contracts if c["side"] == "put")
        if bullish > bearish:
            sentiment = "bullish"
        elif bearish > bullish:
            sentiment = "bearish"
        else:
            sentiment = "neutral"

        max_premium_contract = top_contracts[0] if top_contracts else None
        flow_score = self._compute_flow_score(top_contracts, put_call_ratio, total_vol, total_call_oi + total_put_oi)

        return {
            "has_unusual_activity": len(top_contracts) > 0,
            "unusual_count": len(unusual),
            "flow_score": flow_score,
            "flow_sentiment": sentiment,
            "put_call_ratio": put_call_ratio,
            "call_volume_pct": call_pct,
            "total_option_volume": int(total_vol),
            "total_open_interest": int(total_call_oi + total_put_oi),
            "top_contracts": top_contracts,
            "headline": self._headline(symbol, top_contracts, sentiment, max_premium_contract),
            "current_price": safe_round(current_price),
        }

    def _compute_flow_score(self, top_contracts, put_call_ratio, total_vol, total_oi):
        """0-100 score reflecting strength of unusual flow signal."""
        if not top_contracts:
            return 0
        count_score = clamp(len(top_contracts) * 18)
        avg_vol_oi = statistics.fmean([c["vol_oi_ratio"] for c in top_contracts])
        intensity_score = clamp(min(avg_vol_oi, 20) * 5)
        premium_total = sum(c["premium_total"] for c in top_contracts)
        premium_score = clamp(math.log10(max(premium_total, 1)) * 12)
        return int(
            count_score * 0.30
            + intensity_score * 0.40
            + premium_score * 0.30
        )

    def _headline(self, symbol, top_contracts, sentiment, biggest):
        if not top_contracts:
            return f"{symbol}: no unusual options flow detected"
        total_prem = sum(c["premium_total"] for c in top_contracts)
        prem_str = f"${total_prem:,.0f}" if total_prem >= 1000 else f"${total_prem}"
        return (
            f"{symbol}: {len(top_contracts)} unusual contract(s), "
            f"{prem_str} total premium, {sentiment} skew"
        )

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


class ResearcherAgent:
    def __init__(self, llm_client: OmniRouteClient = None):
        self.llm = llm_client or OmniRouteClient()
        self.market_agent = MarketDataAgent()
        self.alpaca_agent = AlpacaDiscoveryAgent()
        self.news_agent = NewsAgent()
        self.social_agent = SocialSentimentAgent()
        self.fundamentals_agent = FundamentalsAgent()
        self.options_agent = OptionsValidationAgent()
        self.flow_agent = UnusualOptionsFlowAgent()
        self.discovery_agent = SectorDiscoveryAgent()

    def scan_market(self, market_digest: str = "") -> dict:
        scan_config = get_sources_config()
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
        } | set(alpaca_extras) | set(scan_config["market"]["benchmark_symbols"]))

        # ── Phase 2: Deep Analysis ─────────────────────────────
        with ThreadPoolExecutor(max_workers=5) as executor:
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

        themes = self._rank_themes(market, news, social, active_themes, scan_config)
        preliminary = self._rank_preliminary_candidates(
            themes, market, news, social, alpaca, active_themes, scan_config
        )
        top_for_deep_checks = preliminary[:scan_config["fundamentals"]["max_symbols"]]

        deep_check_symbols = [item["ticker"] for item in top_for_deep_checks]
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(
                    timed_agent,
                    "fundamentals",
                    "Fundamentals Agent",
                    lambda: self.fundamentals_agent.run(deep_check_symbols)
                ): "fundamentals",
                executor.submit(
                    timed_agent,
                    "options",
                    "Options Liquidity Agent",
                    lambda: self.options_agent.run(top_for_deep_checks, market.get("metrics", {}))
                ): "options",
                executor.submit(
                    timed_agent,
                    "flow",
                    "Unusual Options Flow Agent",
                    lambda: self.flow_agent.run(deep_check_symbols, market.get("metrics", {}))
                ): "flow",
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
            results.get("flow", {}),
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
                "sources_used": discovery.get("sources_used", []),
                "etfs_configured": discovery_meta.get("etfs_configured", 0),
                "sectors_scanned": discovery_meta.get("sectors_scanned", 0),
                "sectors_active": discovery_meta.get("sectors_active", 0),
                "themes_active": len(active_themes),
                "alpaca_extras": alpaca_extras,
                "etf_rankings": discovery_meta.get("etf_rankings", []),
            },
            "raw_inputs": {
                "market": {
                    "source": market.get("source"),
                    "sources_used": market.get("sources_used", []),
                    "symbols_loaded": len(market.get("metrics", {})),
                    "errors": market.get("errors", {}),
                },
                "news": {
                    "source": news.get("source"),
                    "sources_used": news.get("sources_used", []),
                    "sentiment": news.get("sentiment"),
                    "items_by_theme": news.get("items_by_theme", {}),
                    "errors": news.get("errors", {}),
                },
                "social": {
                    "source": social.get("source"),
                    "sources_used": social.get("sources_used", []),
                    "sentiment": social.get("sentiment"),
                    "items_by_theme": social.get("items_by_theme", {}),
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
                "fundamentals": {
                    "source": results.get("fundamentals", {}).get("source"),
                    "sources_used": results.get("fundamentals", {}).get("sources_used", []),
                    "symbols_loaded": len(results.get("fundamentals", {}).get("fundamentals", {})),
                    "errors": results.get("fundamentals", {}).get("errors", {}),
                },
                "flow": {
                    "source": results.get("flow", {}).get("source"),
                    "symbols_scanned": len(results.get("flow", {}).get("flow_signals", {})),
                    "flagged": sum(
                        1 for sig in results.get("flow", {}).get("flow_signals", {}).values()
                        if sig.get("has_unusual_activity")
                    ),
                    "errors": results.get("flow", {}).get("errors", {}),
                },
            },
            "token_policy": {
                "research_model": "none",
                "final_decision_model": "none",
                "reason": "Rule-based pipeline uses live data sources first; LLM routing is disabled by default for low cost."
            }
        }

    def _rank_themes(self, market, news, social, active_themes, scan_config):
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
            news_score = self._configured_sentiment(news_items, scan_config["news"])
            social_score = self._configured_sentiment(social_items, scan_config["social"])
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

    def _rank_preliminary_candidates(self, themes, market, news, social, alpaca, active_themes, scan_config):
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
                news_score = self._configured_sentiment(news_items, scan_config["news"])
                social_score = self._configured_sentiment(social_items, scan_config["social"])
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

    def _configured_sentiment(self, items, config):
        return sentiment_score(
            [item.get("title") for item in items],
            config["positive_keywords"],
            config["negative_keywords"],
            config["sentiment_word_weight"],
        )

    def _build_watchlist(self, preliminary, fundamentals, options, flow=None):
        fundamentals_config = get_sources_config()["fundamentals"]
        fundamental_by_symbol = fundamentals.get("fundamentals", {})
        option_by_symbol = options.get("validations", {})
        flow_by_symbol = (flow or {}).get("flow_signals", {})

        shortlisted = [
            item for item in preliminary
            if option_by_symbol.get(item["ticker"], {}).get("status") in {"validated", "weak"}
        ]

        # Fetch deep US intelligence (Form 4 Insider, Congress STOCK Act, 8-K, Seasonality, 13F) in parallel
        intel_by_symbol = {}
        if shortlisted:
            with ThreadPoolExecutor(max_workers=min(8, max(1, len(shortlisted)))) as pool:
                future_to_sym = {pool.submit(fetch_all_intelligence, item["ticker"]): item["ticker"] for item in shortlisted}
                for future in as_completed(future_to_sym):
                    sym = future_to_sym[future]
                    try:
                        intel_by_symbol[sym] = future.result()
                    except Exception:
                        intel_by_symbol[sym] = {
                            "symbol": sym,
                            "intelligence_score": 50,
                            "composite_signal": "neutral",
                            "bull_catalysts": [],
                            "bear_catalysts": [],
                        }

        watchlist = []
        for item in shortlisted:
            option_validation = option_by_symbol.get(item["ticker"], {
                "status": "not_checked",
                "score": 0,
                "reason": "Candidate was below deep-check cutoff.",
                "selected_contract": None,
            })
            fundamental = fundamental_by_symbol.get(item["ticker"], {})
            fundamental_score = fundamental.get("fundamental_score", 50)

            intel = intel_by_symbol.get(item["ticker"], {
                "symbol": item["ticker"],
                "intelligence_score": 50,
                "composite_signal": "neutral",
                "bull_catalysts": [],
                "bear_catalysts": [],
            })
            intel_score = intel.get("intelligence_score", 50)
            intel_sig = intel.get("composite_signal", "neutral")

            flow_signal = flow_by_symbol.get(item["ticker"], {})
            flow_score = flow_signal.get("flow_score", 0)
            flow_sentiment = flow_signal.get("flow_sentiment", "neutral")

            preliminary_weight = 0.45
            options_weight = 0.18
            fundamentals_weight = 0.12
            intel_weight = 0.12
            flow_weight = 0.13

            score = clamp(
                item["pre_options_score"] * preliminary_weight
                + option_validation.get("score", 0) * options_weight
                + fundamental_score * fundamentals_weight
                + intel_score * intel_weight
                + flow_score * flow_weight
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
            if flow_signal.get("has_unusual_activity") and flow_sentiment != "neutral":
                bias_aligned = (
                    (flow_sentiment == "bullish" and item["bias"] == "call")
                    or (flow_sentiment == "bearish" and item["bias"] == "put")
                )
                if not bias_aligned:
                    risk_notes.append(f"Unusual options flow is {flow_sentiment}, opposing {item['bias']} bias")

            for bear_note in intel.get("bear_catalysts", []):
                risk_notes.append(bear_note)

            tech_sc = item.get("technical_score", 50)
            news_sc = item.get("news_score", 50)
            opt_sc = option_validation.get("score", 0)
            fund_sc = fundamental_score
            rev_yoy = fundamental.get("revenue_yoy")
            rev_str = f"+{rev_yoy:.1f}% YoY Rev" if rev_yoy is not None else "SEC baseline"

            flow_str = ""
            if flow_signal.get("has_unusual_activity"):
                flow_str = f", {flow_score}/100 unusual flow ({flow_sentiment})"

            selection_reason = (
                f"Selected via active theme '{item.get('theme')}' with {tech_sc}/100 price momentum, "
                f"{news_sc}/100 news sentiment, {rev_str} & {intel_score}/100 intelligence ({intel_sig})"
                f"{flow_str}."
            )
            score_breakdown = (
                f"Score {int(score)}/100 = preliminary multi-source {item['pre_options_score']} (45%) + "
                f"option liquidity {opt_sc} (18%) + SEC fundamentals {fund_sc} (12%) + "
                f"US intelligence {intel_score} (12%) + unusual flow {flow_score} (13%)."
            )

            enriched = dict(item)
            enriched.update({
                "score": int(score),
                "fundamental_score": int(fundamental_score),
                "intelligence_score": int(intel_score),
                "flow_score": int(flow_score),
                "fundamentals": fundamental,
                "intelligence": intel,
                "options": option_validation,
                "flow": flow_signal if flow_signal.get("has_unusual_activity") else {"has_unusual_activity": False, "flow_score": 0},
                "risk_notes": risk_notes,
                "selection_reason": selection_reason,
                "score_breakdown": score_breakdown,
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
        for item in watchlist[:4]:
            contract = item.get("options", {}).get("selected_contract")
            if contract:
                bull_points.append(
                    f"{item['ticker']} has validated {contract['dte']} DTE {contract['type']} liquidity with {contract['open_interest']} OI."
                )
            flow = item.get("flow", {})
            if flow.get("has_unusual_activity"):
                headline = flow.get("headline", "")
                if headline:
                    bull_points.append(headline)
                if flow.get("flow_sentiment") == item.get("bias", "").replace("call", "bullish").replace("put", "bearish"):
                    bull_points.append(f"{item['ticker']} unusual options flow aligns with {item['bias']} bias (flow score {flow.get('flow_score', 0)}/100).")
            for bull_c in item.get("intelligence", {}).get("bull_catalysts", []):
                bull_points.append(bull_c)
            if item.get("risk_notes"):
                bear_points.extend(f"{item['ticker']}: {note}" for note in item["risk_notes"] if not note.startswith(item['ticker']))
                bear_points.extend(note for note in item["risk_notes"] if note.startswith(item['ticker']))

        if spy.get("above_sma50") is False:
            bear_points.append("SPY is below its 50D average, so long premium needs tighter confirmation.")
        if qqq.get("rsi14") and qqq["rsi14"] > 75:
            bear_points.append("QQQ is overbought on 14D RSI.")
        if not bear_points:
            bear_points.append("No severe portfolio-level risk flags in available data.")
        manager = "Watchlist ready; wait for verified TradingView alert before any order."
        if not watchlist:
            manager = "No option-validated watchlist from current data."
        # Remove duplicate points while preserving order
        bull_points = list(dict.fromkeys(bull_points))
        bear_points = list(dict.fromkeys(bear_points))
        return {
            "bull": bull_points[:6],
            "bear": bear_points[:6],
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
            ("flow", "Unusual options flow"),
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
        flow = watch.get("flow", {})
        if flow.get("has_unusual_activity"):
            flow_sentiment = flow.get("flow_sentiment", "neutral")
            bias_match = (
                (flow_sentiment == "bullish" and watch["bias"] == "call")
                or (flow_sentiment == "bearish" and watch["bias"] == "put")
            )
            checks.append({
                "name": "flow_alignment",
                "status": "pass" if bias_match else "warn",
                "message": f"Unusual flow is {flow_sentiment} (bias: {watch['bias']}), score {flow.get('flow_score', 0)}/100",
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
