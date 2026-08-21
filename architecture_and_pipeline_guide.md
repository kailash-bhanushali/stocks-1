# Complete Pipeline Architecture & Agent Flow Guide

This guide explains every stage of the Research-First Options Trading Platform, how data flows through the system, where agents execute, and what is required to make the platform production-ready.

---

## 1. System Architecture & Activity Flow Diagram

```mermaid
flowchart TD
    subgraph PHASE_1 ["Phase 1: Market & Sector Discovery"]
        A1["1. Sector Discovery Agent"] -->|Scans 19 Sector ETFs| A2["Identifies Top 5 Hot Sectors"]
        A3["Alpaca Screener Agent"] -->|Top Movers & Actives| A4["Discovers Volatile Extras"]
        A2 & A4 --> B1["Dynamic Candidate Universe (25-40 Tickers)"]
    end

    subgraph PHASE_2 ["Phase 2: Parallel Deep Research"]
        B1 --> C1["2. Market Data Agent"]
        B1 --> C2["3. News Agent"]
        B1 --> C3["4. Social Sentiment Agent"]
        B1 --> C4["5. Fundamentals Agent"]
        
        C1 -->|SMA20/50, RSI, 5D/20D Returns| D1["Quant Metrics"]
        C2 -->|Financial Headlines| D2["News Score"]
        C3 -->|Reddit & HackerNews| D3["Social Buzz"]
        C4 -->|SEC XBRL Filings| D4["YoY Revenue Growth"]
    end

    subgraph PHASE_3 ["Phase 3: Options Validation & Theme Ranking"]
        D1 & D2 & D3 & D4 --> E1["6. Options Liquidity Agent"]
        E1 -->|Checks DTE 25-65, Spread %, OI| E2["Option-Validated Watchlist"]
        E2 --> E3["7. Theme Ranker & Research Team Debate"]
        E3 -->|Bull vs Bear vs Risk Manager| E4["Watchlist + TradingView Alert Triggers"]
    end

    subgraph PHASE_4 ["Phase 4: Live Data & Engine Integration"]
        E4 --> F1["8. Data Feed Router"]
        E4 --> F2["9. Alpaca Option Stream"]
        E4 --> F3["10. QuantConnect LEAN Export"]
    end

    subgraph PHASE_5 ["Phase 5: Signal Confirmation & Order Execution"]
        G1["TradingView Webhook Alert"] -->|Verifies Secret HMAC| G2["11. Technical Confirmation Agent"]
        E4 & G2 --> G3["12. Trader Agent Gate"]
        G3 -->|Passes Risk Checks & Confidence > 65%| G4["13. Alpaca Execution Engine"]
    end
```

---

## 2. Step-by-Step Breakdown of the 12 Pipeline Boxes

| # | Stage Name | What it Does | Inputs / Source | Output / Result | Agent Running |
|---|---|---|---|---|---|
| **1** | **Discovery** | Scans 19 sector ETFs (XLK, XLE, IBB, SMH, etc.) for 5D/20D price returns & volume expansion. Finds hot market themes. | Yahoo Finance ETF charts + Alpaca screeners | Top 5 active sectors + 25-40 dynamic tickers | `SectorDiscoveryAgent` |
| **2** | **Market** | Computes technical momentum metrics for each discovered ticker: 20D/50D Moving Averages, RSI, 5D/20D/60D returns, volume ratio. | Yahoo Finance price bars | `technical_score` (0–100) per ticker | `MarketDataAgent` |
| **3** | **Sources** | Gathers qualitative text & SEC corporate filings. Measures headline sentiment and YoY revenue/net income growth. | Yahoo News, Reddit RSS, HackerNews, SEC XBRL API | `news_score`, `social_score`, `fundamental_score` | `NewsAgent` + `SocialSentimentAgent` + `FundamentalsAgent` |
| **4** | **Research Team** | Runs a multi-perspective debate synthesizing bull arguments, bear risks, and portfolio risk regime (SPY/QQQ health). | Combined Market + Sources outputs | Structured Bull / Bear / Risk Manager debate | `ResearcherAgent` (Debate module) |
| **5** | **Options** | Validates options chain liquidity. Filters for contract DTE (25–65 days), tight bid/ask spread (<18%), and open interest (>100). | Cboe option chain data / Alpaca data | Selected OCC Contract (e.g. `PLTR231215C00020000`) | `OptionsValidationAgent` |
| **6** | **Themes** | Ranks active market baskets by combining sector momentum, volume surge, and narrative sentiment. | Active sector scores | Ranked themes with strength (0–100) | `ResearcherAgent` (_rank_themes) |
| **7** | **Watchlist** | Filters the top 8 candidates that passed both research & options liquidity checks. Generates custom TradingView alert rules. | Preliminary candidates + Options validation | Top 8 Watchlist with custom TV alert JSON | `ResearcherAgent` (_build_watchlist) |
| **8** | **Data Feeds** | Route-checks live stock and option quotes across configured providers (Alpaca, Finnhub, Alpha Vantage). | Provider API status | Quote provider health snapshot | `DataFeedRouter` |
| **9** | **Option Stream** | Establishes a live WebSocket stream to receive real-time bid/ask price updates for watchlist option contracts. | Alpaca WebSocket Feed | Live options quote stream (`bid`, `ask`, `last`) | `alpaca_option_stream` |
| **10** | **LEAN Engine** | Exports the option-validated universe into QuantConnect LEAN format (`universe.json`) for local backtesting & dry-running. | Watchlist candidates | `lean/universe.json` export | `lean_adapter.py` |
| **11** | **TradingView** | Listens on `/webhook/tradingview` for incoming indicator alerts. Verifies the secret token and matches alert against watchlist bias. | TradingView Webhook HTTP POST | Verified signal confirmation (`confirmed` / `rejected`) | `TechnicalConfirmationAgent` |
| **12** | **Decision Gate & Execution** | Evaluates confirmed signals against account buying power, option premium caps, and risk rules. Places paper/live orders via Alpaca. | Confirmed signal + Risk rules | Filled Alpaca order or paper plan log | `TraderAgent` + `alpaca_service` |

---

## 3. Agent Architecture: Who Runs Where?

Total Agents in System: **8 Specialized Agents**

### Background Research Agents (Run automatically or on demand)
1. **`SectorDiscoveryAgent`**: Discovers hot sectors & movers (Phase 1).
2. **`AlpacaDiscoveryAgent`**: Queries Alpaca market movers screener (Phase 1).
3. **`MarketDataAgent`**: Fetches technical chart bars & indicators (Phase 2).
4. **`NewsAgent`**: Pulls financial news headlines (Phase 2).
5. **`SocialSentimentAgent`**: Monitors Reddit & HackerNews discussions (Phase 2).
6. **`FundamentalsAgent`**: Queries SEC company facts CIK database (Phase 2).
7. **`OptionsValidationAgent`**: Checks option chain liquidity & DTE filters (Phase 3).

### Signal & Execution Agents (Run when a TradingView alert arrives)
8. **`TechnicalConfirmationAgent`**: Verifies HMAC secret & technical signal alignment.
9. **`TraderAgent`**: Runs final risk gate checks (premium cap, spread, market regime) and submits orders.

---

## 4. Production Readiness Roadmap: What is Needed When?

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        PRODUCTION READINESS PHASES                          │
├──────────────────────────┬───────────────────────┬─────────────────────────┤
│ Phase 1: Current (Done)  │ Phase 2: AI Reasoning │ Phase 3: Live Trading   │
├──────────────────────────┼───────────────────────┼─────────────────────────┤
│ • Dynamic Discovery      │ • Replace mock LLM    │ • Set ALPACA_TRADING=   │
│ • Quant Market Filters   │   with OmniRoute API  │   true                  │
│ • Options Chain Check    │ • LLM Bull/Bear       │ • Cloud HTTPS Webhook   │
│ • TV Webhook Receiver    │   Debate Synthesis    │   (ngrok / AWS / VPS)   │
│ • LEAN Docker Export     │ • LLM Risk Manager    │ • Live Option Stream    │
└──────────────────────────┴───────────────────────┴─────────────────────────┘
```

### Critical Checklist for Production Deployment:

1. **HTTPS Webhook URL (Required for TradingView)**:
   - TradingView cannot send webhooks to `http://127.0.0.1:8080`.
   - Production requires exposing `/webhook/tradingview` over HTTPS using a domain or tool like `ngrok`, Cloudflare Tunnel, or deploying to a VPS (AWS / DigitalOcean).

2. **Secret Key Verification (`TRADINGVIEW_WEBHOOK_SECRET`)**:
   - Set a strong secret in `.env` and include it in your TradingView alert JSON payload to block unauthorized requests.

3. **LLM Integration via OmniRoute**:
   - Plug in your OmniRoute / OpenRouter API keys to enable LLM-powered Bull/Bear debates and final decision reasoning.

4. **Alpaca Trading Toggle (`ALPACA_TRADING_ENABLED`)**:
   - Currently, orders are generated as **"test plans"**. Set `ALPACA_TRADING_ENABLED=true` in `.env` when ready to place real paper/live trades.
