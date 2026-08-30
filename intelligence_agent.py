"""
intelligence_agent.py
─────────────────────
US Market Intelligence: free data from SEC EDGAR, Yahoo Finance, and Finnhub.

Provides per-ticker deep intelligence:
  1. Insider trades     – SEC EDGAR Form 4 (corporate insiders: officers, directors)
  2. Congress trades    – House Clerk STOCK Act disclosures + Senate eFD
  3. Corporate actions  – SEC EDGAR 8-K events + Yahoo Finance splits/dividends
  4. Ownership          – SEC EDGAR 13F institutional filers (top holders)
  5. Seasonality        – Yahoo Finance OHLCV → monthly average return over 5 years

All sources are public and free. No API key required.
Optional: FINNHUB_API_KEY and FMP_API_KEY for higher-quality fallbacks.
"""

from datetime import datetime, timezone, timedelta
import json
import os
import re
import time
import urllib.parse
from typing import Optional

import requests

_HEADERS = {
    "User-Agent": os.getenv(
        "RESEARCH_USER_AGENT",
        "trading-research-bot/0.1 contact:kailash-local"
    ),
    "Accept": "application/json",
}

_CACHE: dict = {}  # symbol → {key: {data, expires}}
_CACHE_TTL = 3600  # 1 hour – SEC EDGAR rate-limits if you hammer it


# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────

def _cache_get(symbol: str, key: str):
    entry = _CACHE.get(symbol, {}).get(key)
    if entry and entry["expires"] > time.monotonic():
        return entry["data"]
    return None


def _cache_set(symbol: str, key: str, data):
    _CACHE.setdefault(symbol, {})[key] = {
        "data": data,
        "expires": time.monotonic() + _CACHE_TTL,
    }


def _get(url: str, params: dict = None, timeout: int = 10):
    try:
        resp = requests.get(url, params=params, headers=_HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def _yahoo_bars(symbol: str, range_: str = "5y", interval: str = "1mo") -> list[dict]:
    """Fetch OHLCV bars from Yahoo Finance (no auth needed)."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol)}"
    data = _get(url, params={"range": range_, "interval": interval, "events": "div,split"})
    if not data:
        return []
    try:
        result = data["chart"]["result"][0]
        timestamps = result.get("timestamp", [])
        closes = result["indicators"]["quote"][0].get("close", [])
        opens  = result["indicators"]["quote"][0].get("open", [])
        bars = []
        for i, ts in enumerate(timestamps):
            c = closes[i] if i < len(closes) else None
            o = opens[i]  if i < len(opens)  else None
            if ts and c is not None and o is not None:
                bars.append({
                    "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m"),
                    "open": round(o, 2),
                    "close": round(c, 2),
                    "return_pct": round((c - o) / o * 100, 2) if o else 0,
                })
        return bars
    except Exception:
        return []


def _sec_cik(symbol: str) -> Optional[str]:
    """Resolve ticker symbol → SEC CIK (11-digit string)."""
    cached = _cache_get(symbol, "cik")
    if cached:
        return cached
    data = _get("https://efts.sec.gov/LATEST/search-index?q=%22" +
                urllib.parse.quote(symbol) + "%22&dateRange=custom&startdt=2020-01-01&forms=10-K")
    # Preferred: use the tickers.json index
    tickers = _get("https://www.sec.gov/files/company_tickers.json")
    if tickers:
        for _k, v in tickers.items():
            if v.get("ticker", "").upper() == symbol.upper():
                cik = str(v["cik_str"]).zfill(10)
                _cache_set(symbol, "cik", cik)
                return cik
    return None


# ─────────────────────────────────────────────────────────────────
# 1. Insider Trades – SEC EDGAR Form 4
# ─────────────────────────────────────────────────────────────────

def fetch_insider_trades(symbol: str) -> dict:
    """
    Returns last 20 insider trades from SEC EDGAR Form 4 filings.
    Includes: person name, title, transaction type, shares, price, date, signal.
    """
    cached = _cache_get(symbol, "insider")
    if cached:
        return cached

    cik = _sec_cik(symbol)
    if not cik:
        result = {"symbol": symbol, "source": "SEC EDGAR Form 4", "trades": [], "signal": "no_cik", "summary": "CIK not found for this ticker"}
        _cache_set(symbol, "insider", result)
        return result

    # Fetch recent Form 4 filings via EDGAR full-text search
    url = "https://efts.sec.gov/LATEST/search-index"
    params = {
        "q": f'"{symbol}"',
        "forms": "4",
        "dateRange": "custom",
        "startdt": (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
        "enddt": datetime.now().strftime("%Y-%m-%d"),
    }
    data = _get(url, params=params)

    # Simpler approach: use EDGAR submissions endpoint
    submissions = _get(f"https://data.sec.gov/submissions/CIK{cik}.json")
    trades = []
    signal = "neutral"

    if submissions:
        filings = submissions.get("filings", {}).get("recent", {})
        forms  = filings.get("form", [])
        dates  = filings.get("filingDate", [])
        acces  = filings.get("accessionNumber", [])
        descs  = filings.get("primaryDocument", [])
        report_dates = filings.get("reportDate", [])

        buy_count = 0
        sell_count = 0
        for i, form in enumerate(forms):
            if form != "4":
                continue
            if len(trades) >= 20:
                break
            filing_date = dates[i] if i < len(dates) else "?"
            acc = acces[i].replace("-", "") if i < len(acces) else ""
            doc = descs[i] if i < len(descs) else ""
            report_date = report_dates[i] if i < len(report_dates) else filing_date

            # Fetch the actual form4 document to extract trade details
            trade_info = _parse_form4(cik, acc, doc, report_date, filing_date)
            if trade_info:
                trades.extend(trade_info)
                for t in trade_info:
                    if t.get("transaction_type") in ("P", "Purchase", "Buy"):
                        buy_count += 1
                    elif t.get("transaction_type") in ("S", "Sale", "Sell"):
                        sell_count += 1

        if buy_count >= 3:
            signal = "bullish"
        elif sell_count >= 5 and buy_count == 0:
            signal = "bearish"
        elif buy_count > 0 or sell_count > 0:
            signal = "mixed"

    summary = _insider_summary(trades, signal)
    result = {
        "symbol": symbol,
        "source": "SEC EDGAR Form 4",
        "source_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=4&dateb=&owner=include&count=40",
        "trades": trades[:20],
        "signal": signal,
        "summary": summary,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(symbol, "insider", result)
    return result


def _parse_form4(cik: str, accession: str, doc: str, report_date: str, filing_date: str) -> list[dict]:
    """Fetch and parse a Form 4 filing XML to extract individual transactions."""
    if not accession or not doc:
        return []
    try:
        cik_padded = cik.zfill(10)
        acc_dashed = f"{accession[:10]}-{accession[10:12]}-{accession[12:]}" if len(accession) == 18 else accession
        # Try the XML file first
        xml_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/{doc}"
        resp = requests.get(xml_url, headers=_HEADERS, timeout=8)
        if resp.status_code != 200:
            return []
        text = resp.text

        # Parse key fields using regex (lightweight; avoids xml namespace issues)
        trades = []
        # Reporter name
        name_m = re.search(r'<rptOwnerName>(.*?)</rptOwnerName>', text, re.S)
        name = name_m.group(1).strip() if name_m else "Unknown"
        title_m = re.search(r'<officerTitle>(.*?)</officerTitle>', text, re.S)
        title = title_m.group(1).strip() if title_m else ""

        # Find all non-derivative transactions
        for tx_block in re.findall(r'<nonDerivativeTransaction>(.*?)</nonDerivativeTransaction>', text, re.S):
            tcode_m  = re.search(r'<transactionCode>(.*?)</transactionCode>', tx_block)
            shares_m = re.search(r'<transactionShares>\s*<value>(.*?)</value>', tx_block, re.S)
            price_m  = re.search(r'<transactionPricePerShare>\s*<value>(.*?)</value>', tx_block, re.S)
            date_m   = re.search(r'<transactionDate>\s*<value>(.*?)</value>', tx_block, re.S)

            tcode  = tcode_m.group(1).strip() if tcode_m  else "?"
            shares = shares_m.group(1).strip() if shares_m else "?"
            price  = price_m.group(1).strip()  if price_m  else "?"
            tdate  = date_m.group(1).strip()   if date_m   else report_date

            tx_type = {"P": "Buy", "S": "Sell", "A": "Award", "D": "Disposition",
                       "G": "Gift", "F": "Tax Withholding", "M": "Option Exercise"}.get(tcode, tcode)

            try:
                shares_num = float(shares.replace(",", ""))
                price_num  = float(price.replace(",", "")) if price != "?" else None
                value_str  = f"${shares_num * price_num:,.0f}" if price_num else "?"
            except Exception:
                value_str = "?"
                shares_num = 0

            # Signal reasoning
            if tcode == "P":
                reason = f"{name} ({title or 'Insider'}) purchased {shares} shares @ ${price} — direct insider buy is a bullish signal."
            elif tcode == "S":
                reason = f"{name} ({title or 'Insider'}) sold {shares} shares @ ${price} — may indicate profit-taking or portfolio rebalancing."
            elif tcode == "A":
                reason = f"{name} ({title or 'Insider'}) received {shares} shares as compensation award."
            else:
                reason = f"{name} ({title or 'Insider'}) — transaction code {tcode}: {shares} shares."

            trades.append({
                "person": name,
                "title": title,
                "transaction_type": tx_type,
                "transaction_code": tcode,
                "shares": shares,
                "price": price,
                "total_value": value_str,
                "date": tdate,
                "filing_date": filing_date,
                "reason": reason,
            })
    except Exception:
        return []
    return trades


def _insider_summary(trades: list, signal: str) -> str:
    if not trades:
        return "No Form 4 insider trades in the last 180 days."
    buys = [t for t in trades if t.get("transaction_code") == "P"]
    sells = [t for t in trades if t.get("transaction_code") == "S"]
    parts = []
    if buys:
        parts.append(f"{len(buys)} insider purchase(s)")
    if sells:
        parts.append(f"{len(sells)} insider sale(s)")
    sig_text = {"bullish": "→ Cluster buy pattern is bullish signal",
                "bearish": "→ Heavy selling pressure from insiders",
                "mixed":   "→ Mixed insider activity",
                "neutral": "→ Minimal insider activity"}.get(signal, "")
    return f"{', '.join(parts)} in last 180 days. {sig_text}"


# ─────────────────────────────────────────────────────────────────
# 2. Congress / Politician Trades – House Clerk STOCK Act
# ─────────────────────────────────────────────────────────────────

def fetch_congress_trades(symbol: str) -> dict:
    """
    Fetches recent congressional stock trades for this ticker.
    Uses the Quiver Quant public endpoint (no auth for basic data) and
    the Capitol Trades public JSON as fallback.
    Note: STOCK Act trades can be disclosed up to 45 days late.
    """
    cached = _cache_get(symbol, "congress")
    if cached:
        return cached

    trades = []

    # Try Capitol Trades (public JSON API)
    ct_url = "https://www.capitoltrades.com/api/trades"
    ct_params = {"symbol": symbol.upper(), "pageSize": 20, "page": 1}
    ct_data = _get(ct_url, params=ct_params, timeout=8)
    if ct_data and isinstance(ct_data, dict) and ct_data.get("data"):
        for item in ct_data["data"][:20]:
            tx_type = item.get("txType", "?")
            amount  = item.get("amount", {})
            amount_label = f"${amount.get('gte', '?'):,} – ${amount.get('lte', '?'):,}" if isinstance(amount, dict) else str(amount)
            member = item.get("politician", {}) if isinstance(item.get("politician"), dict) else {}
            name = member.get("firstName", "") + " " + member.get("lastName", "")
            party_state = f"{member.get('party', '?')}-{member.get('state', '?')}"
            traded_on = item.get("traded", "?")
            disclosed_on = item.get("published", "?")

            if tx_type.lower() in ("buy", "purchase"):
                reason = f"Rep. {name.strip()} ({party_state}) purchased. Congress buys often follow positive committee briefings or sector-favorable legislation."
            else:
                reason = f"Rep. {name.strip()} ({party_state}) sold. Disclosure lag: trade on {traded_on}, reported {disclosed_on}."

            trades.append({
                "person": name.strip(),
                "chamber": member.get("chamber", "?"),
                "party_state": party_state,
                "transaction_type": tx_type,
                "amount_range": amount_label,
                "traded_on": traded_on,
                "disclosed_on": disclosed_on,
                "reason": reason,
            })

    # Fallback: check Quiver Quant public API (no key for limited data)
    if not trades:
        qv_url = f"https://api.quiverquant.com/beta/historical/congresstrading/{symbol.upper()}"
        qv_data = _get(qv_url, timeout=6)
        if isinstance(qv_data, list):
            for item in qv_data[:20]:
                tx = item.get("Transaction", "?")
                name = item.get("Representative", "Unknown")
                party = item.get("Party", "?")
                chamber = item.get("Chamber", "?")
                date = item.get("TransactionDate", "?")
                amount = item.get("Amount", "?")
                reason = (
                    f"Rep. {name} ({party}, {chamber}) — {tx} worth {amount}. "
                    "Congressional trade filed under the STOCK Act."
                )
                trades.append({
                    "person": name,
                    "chamber": chamber,
                    "party_state": f"{party}-{item.get('State', '?')}",
                    "transaction_type": tx,
                    "amount_range": str(amount),
                    "traded_on": date,
                    "disclosed_on": item.get("DisclosureDate", "?"),
                    "reason": reason,
                })

    signal = "neutral"
    buys  = [t for t in trades if "buy" in t.get("transaction_type", "").lower() or "purchase" in t.get("transaction_type", "").lower()]
    sells = [t for t in trades if "sell" in t.get("transaction_type", "").lower() or "sale" in t.get("transaction_type", "").lower()]
    if len(buys) >= 2:
        signal = "bullish"
    elif len(sells) > len(buys) * 2:
        signal = "bearish"
    elif trades:
        signal = "mixed"

    if trades:
        summary = f"{len(buys)} buy(s), {len(sells)} sell(s) by congress members. Trades disclosed up to 45 days late."
    else:
        summary = "No recent congressional trades found for this ticker. Data from House Clerk / Capitol Trades."

    result = {
        "symbol": symbol,
        "source": "STOCK Act — House Clerk & Capitol Trades",
        "source_url": f"https://www.capitoltrades.com/trades?ticker={symbol}",
        "trades": trades,
        "signal": signal,
        "summary": summary,
        "note": "Congress members must disclose trades within 45 days. Data may lag real-time.",
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(symbol, "congress", result)
    return result


# ─────────────────────────────────────────────────────────────────
# 3. Corporate Actions – SEC EDGAR 8-K + Yahoo Finance
# ─────────────────────────────────────────────────────────────────

def fetch_corporate_actions(symbol: str) -> dict:
    """
    Fetches recent corporate actions:
    - SEC EDGAR 8-K filings (earnings, M&A, guidance, restructuring)
    - Yahoo Finance splits and dividends
    """
    cached = _cache_get(symbol, "corporate_actions")
    if cached:
        return cached

    cik = _sec_cik(symbol)
    actions = []

    # 1. 8-K filings from EDGAR submissions
    if cik:
        submissions = _get(f"https://data.sec.gov/submissions/CIK{cik}.json")
        if submissions:
            filings = submissions.get("filings", {}).get("recent", {})
            forms  = filings.get("form", [])
            dates  = filings.get("filingDate", [])
            descs  = filings.get("items", [])  # items field has 8-K item codes
            accdocs= filings.get("primaryDocument", [])

            cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            for i, form in enumerate(forms):
                if form not in ("8-K", "8-K/A"):
                    continue
                filing_date = dates[i] if i < len(dates) else "?"
                if filing_date < cutoff:
                    break
                items_str = descs[i] if i < len(descs) else ""
                event_type = _classify_8k(items_str)
                reason = _8k_reason(event_type, items_str, filing_date)

                actions.append({
                    "type": event_type,
                    "date": filing_date,
                    "items": items_str,
                    "source": "SEC EDGAR 8-K",
                    "reason": reason,
                })
                if len(actions) >= 20:
                    break

    # 2. Yahoo splits/dividends
    yahoo_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol)}"
    yahoo_data = _get(yahoo_url, params={"range": "2y", "interval": "1d", "events": "div,split"})
    if yahoo_data:
        try:
            result_data = yahoo_data["chart"]["result"][0]
            events = result_data.get("events", {})
            for ts_str, div in events.get("dividends", {}).items():
                ts = int(ts_str)
                date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                amount = div.get("amount", "?")
                actions.append({
                    "type": "Dividend",
                    "date": date_str,
                    "items": f"${amount:.4f}" if isinstance(amount, float) else str(amount),
                    "source": "Yahoo Finance",
                    "reason": f"Dividend of ${amount:.4f}/share paid. Regular dividends signal financial health.",
                })
            for ts_str, split in events.get("splits", {}).items():
                ts = int(ts_str)
                date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                ratio = split.get("splitRatio", "?")
                actions.append({
                    "type": "Stock Split",
                    "date": date_str,
                    "items": str(ratio),
                    "source": "Yahoo Finance",
                    "reason": f"Stock split {ratio}. Splits typically broaden retail access and signal management confidence.",
                })
        except Exception:
            pass

    # Sort by date descending
    actions.sort(key=lambda x: x.get("date", ""), reverse=True)

    result = {
        "symbol": symbol,
        "source": "SEC EDGAR 8-K + Yahoo Finance",
        "source_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik or symbol}&type=8-K&dateb=&owner=include&count=40",
        "actions": actions[:20],
        "summary": f"{len(actions)} corporate actions in the last 12 months ({len([a for a in actions if a['type'] == 'Dividend'])} dividends, {len([a for a in actions if '8-K' in a.get('source', '')])} 8-K filings).",
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(symbol, "corporate_actions", result)
    return result


def _classify_8k(items_str: str) -> str:
    mapping = {
        "1.01": "M&A / Material Agreement",
        "1.02": "Termination of Agreement",
        "2.01": "Acquisition / Disposition",
        "2.02": "Earnings Results",
        "2.03": "Off-Balance Sheet",
        "2.05": "Departure / Executive Changes",
        "5.02": "Executive Changes",
        "7.01": "Regulation FD / Guidance",
        "8.01": "Other Material Event",
        "9.01": "Financial Statements",
    }
    for code, label in mapping.items():
        if code in items_str:
            return label
    return "Material Event 8-K"


def _8k_reason(event_type: str, items_str: str, date: str) -> str:
    reasons = {
        "Earnings Results":       f"Company filed earnings results ({date}). Watch for beats/misses vs estimates.",
        "M&A / Material Agreement": f"Material agreement or M&A activity disclosed ({date}). Can be a significant catalyst.",
        "Executive Changes":      f"Executive departure or hire disclosed ({date}). Leadership changes can affect strategy.",
        "Regulation FD / Guidance": f"Management guidance or Reg FD disclosure ({date}). Watch for forward-looking signals.",
        "Acquisition / Disposition": f"Asset acquisition or disposition ({date}). Can shift company fundamentals.",
    }
    return reasons.get(event_type, f"Material 8-K event filed {date}. Review item codes: {items_str}.")


# ─────────────────────────────────────────────────────────────────
# 4. Ownership – SEC EDGAR 13F Institutional Filers
# ─────────────────────────────────────────────────────────────────

def fetch_ownership(symbol: str) -> dict:
    """
    Returns top institutional owners from SEC EDGAR 13F filings.
    Uses Finnhub (if key configured) or SEC EDGAR 13F EFTS search as fallback.
    """
    cached = _cache_get(symbol, "ownership")
    if cached:
        return cached

    owners = []

    # Try Finnhub (free tier, richer data)
    finnhub_key = os.getenv("FINNHUB_API_KEY", "")
    if finnhub_key:
        fh_url = "https://finnhub.io/api/v1/stock/ownership"
        fh_data = _get(fh_url, params={"symbol": symbol.upper(), "token": finnhub_key}, timeout=8)
        if fh_data and isinstance(fh_data, dict):
            for holder in fh_data.get("ownership", [])[:10]:
                name = holder.get("name", "Unknown")
                shares = holder.get("share", 0)
                pct = holder.get("percentOwned", 0)
                change = holder.get("change", 0)
                direction = "increased" if change > 0 else ("decreased" if change < 0 else "unchanged")
                reason = (
                    f"{name} holds {pct:.2f}% ({shares:,} shares). "
                    f"Stake {direction} by {abs(change):,} shares this quarter."
                )
                owners.append({
                    "name": name,
                    "shares": shares,
                    "percent": round(pct, 4),
                    "change_shares": change,
                    "direction": direction,
                    "reason": reason,
                    "source": "Finnhub 13F",
                })

    # Fallback: SEC EDGAR 13F EFTS full-text search for this ticker
    if not owners:
        try:
            efts_url = "https://efts.sec.gov/LATEST/search-index"
            efts_params = {
                "q": f'"{symbol}"',
                "forms": "13F-HR",
                "dateRange": "custom",
                "startdt": (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d"),
                "enddt": datetime.now().strftime("%Y-%m-%d"),
            }
            efts_data = _get(efts_url, params=efts_params, timeout=10)
            if efts_data and isinstance(efts_data, dict):
                hits = efts_data.get("hits", {}).get("hits", [])
                seen = set()
                for hit in hits[:15]:
                    src = hit.get("_source", {})
                    display_names = src.get("display_names", [])
                    entity = display_names[0].split("(CIK")[0].strip() if display_names else "Unknown Fund"
                    if entity in seen:
                        continue
                    seen.add(entity)
                    filed = src.get("file_date", "?")
                    period = src.get("period_ending", "?")
                    owners.append({
                        "name": entity,
                        "shares": "—",
                        "percent": 0.0,
                        "change_shares": 0,
                        "direction": "unknown",
                        "reason": (
                            f"{entity} holds AAPL in a 13F-HR filed {filed} "
                            f"(period ending {period}). "
                            f"Add FINNHUB_API_KEY for exact share counts and % held."
                        ),
                        "source": "SEC EDGAR 13F-HR",
                    })
        except Exception:
            pass


    total_inst_pct = sum(o.get("percent", 0) for o in owners if isinstance(o.get("percent"), (int, float)))
    result = {
        "symbol": symbol,
        "source": "SEC EDGAR 13F via Finnhub / EDGAR EFTS",
        "source_url": f"https://efts.sec.gov/LATEST/search-index?q=%22{symbol}%22&forms=13F-HR",
        "owners": owners,
        "total_institutional_pct": round(total_inst_pct, 2),
        "summary": f"Top {len(owners)} institutional filers identified from recent 13F-HR filings." if owners else "No institutional ownership data found. Add FINNHUB_API_KEY to .env for detailed ownership.",
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(symbol, "ownership", result)
    return result


# ─────────────────────────────────────────────────────────────────
# 5. Seasonality – Yahoo Finance Monthly Returns
# ─────────────────────────────────────────────────────────────────

def fetch_seasonality(symbol: str) -> dict:
    """
    Computes average monthly return (%) over 5 years of Yahoo Finance data.
    Returns 12-month profile: best month, worst month, and full monthly breakdown.
    """
    cached = _cache_get(symbol, "seasonality")
    if cached:
        return cached

    bars = _yahoo_bars(symbol, range_="5y", interval="1mo")
    if not bars:
        result = {
            "symbol": symbol,
            "source": "Yahoo Finance (5y monthly bars)",
            "months": [],
            "best_month": None,
            "worst_month": None,
            "summary": "Seasonality data unavailable.",
            "as_of": datetime.now(timezone.utc).isoformat(),
        }
        _cache_set(symbol, "seasonality", result)
        return result

    MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    monthly_returns: dict[int, list[float]] = {m: [] for m in range(1, 13)}

    for bar in bars:
        try:
            month_num = int(bar["date"].split("-")[1])
            monthly_returns[month_num].append(bar["return_pct"])
        except Exception:
            continue

    months = []
    for m in range(1, 13):
        returns = monthly_returns[m]
        avg_return = round(sum(returns) / len(returns), 2) if returns else 0
        positive_years = sum(1 for r in returns if r > 0)
        months.append({
            "month": MONTH_NAMES[m - 1],
            "month_num": m,
            "avg_return_pct": avg_return,
            "years_positive": positive_years,
            "sample_size": len(returns),
            "reason": (
                f"{MONTH_NAMES[m-1]}: avg {avg_return:+.2f}% over {len(returns)} years "
                f"({positive_years}/{len(returns)} years positive). "
                + ("Historically strong month." if avg_return > 1.5 else
                   "Historically weak month." if avg_return < -1.0 else
                   "Neutral historical month.")
            ),
        })

    best  = max(months, key=lambda x: x["avg_return_pct"])
    worst = min(months, key=lambda x: x["avg_return_pct"])

    result = {
        "symbol": symbol,
        "source": "Yahoo Finance (5-year monthly bars)",
        "months": months,
        "best_month": best,
        "worst_month": worst,
        "summary": (
            f"Best month: {best['month']} (avg {best['avg_return_pct']:+.2f}%). "
            f"Worst month: {worst['month']} (avg {worst['avg_return_pct']:+.2f}%). "
            f"Based on 5 years of data."
        ),
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(symbol, "seasonality", result)
    return result


# ─────────────────────────────────────────────────────────────────
# 6. All-in-One Fetch
# ─────────────────────────────────────────────────────────────────

def fetch_all_intelligence(symbol: str) -> dict:
    """
    Fetches all 5 intelligence modules in parallel threads.
    Returns a single dict with all data + a top-level signal summary.
    """
    import threading

    results = {}
    errors  = {}

    def run(key, fn):
        try:
            results[key] = fn(symbol)
        except Exception as exc:
            errors[key] = str(exc)
            results[key] = {"error": str(exc)}

    threads = [
        threading.Thread(target=run, args=("insider",          fetch_insider_trades)),
        threading.Thread(target=run, args=("congress",         fetch_congress_trades)),
        threading.Thread(target=run, args=("corporate_actions",fetch_corporate_actions)),
        threading.Thread(target=run, args=("ownership",        fetch_ownership)),
        threading.Thread(target=run, args=("seasonality",      fetch_seasonality)),
    ]
    deadline = time.monotonic() + 10.0
    for t in threads:
        t.start()
    for t in threads:
        remaining = max(0.05, deadline - time.monotonic())
        t.join(timeout=remaining)

    # Composite signal
    signals = {
        "insider":   results.get("insider",  {}).get("signal", "neutral"),
        "congress":  results.get("congress", {}).get("signal", "neutral"),
    }
    bullish_count = sum(1 for s in signals.values() if s == "bullish")
    bearish_count = sum(1 for s in signals.values() if s == "bearish")
    composite = "bullish" if bullish_count > bearish_count else ("bearish" if bearish_count > bullish_count else "neutral")

    return {
        "symbol": symbol,
        "composite_signal": composite,
        "signals": signals,
        "insider": results.get("insider"),
        "congress": results.get("congress"),
        "corporate_actions": results.get("corporate_actions"),
        "ownership": results.get("ownership"),
        "seasonality": results.get("seasonality"),
        "errors": errors,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def compute_intelligence_scoring(symbol: str, full_intel: Optional[dict] = None) -> dict:
    """
    Computes a quantitative score (0-100) and extracts bullish/bearish catalysts
    from Insider, Congress, Corporate Actions, Ownership, and Seasonality data.
    """
    if full_intel is None:
        try:
            full_intel = fetch_all_intelligence(symbol)
        except Exception:
            full_intel = {}

    insider = full_intel.get("insider") or {}
    congress = full_intel.get("congress") or {}
    actions = full_intel.get("corporate_actions") or {}
    ownership = full_intel.get("ownership") or {}
    seasonality = full_intel.get("seasonality") or {}

    score = 50.0  # neutral baseline
    bull_catalysts = []
    bear_catalysts = []

    # 1. Insider Trades (Form 4)
    in_sig = insider.get("signal", "neutral")
    in_trades = insider.get("trades") or []
    buys = [t for t in in_trades if t.get("transaction_code") == "P" or t.get("transaction_type") == "Buy"]
    sells = [t for t in in_trades if t.get("transaction_code") == "S" or t.get("transaction_type") == "Sell"]
    if in_sig == "bullish" or len(buys) >= 2:
        score += 12.0
        bull_catalysts.append(f"{symbol}: Form 4 cluster insider buying ({len(buys)} purchase{'s' if len(buys)>1 else ''} in 180d)")
    elif in_sig == "bearish" or len(sells) >= 5:
        score -= 10.0
        bear_catalysts.append(f"{symbol}: Heavy insider selling ({len(sells)} sales in 180d)")
    elif buys and not sells:
        score += 6.0
        bull_catalysts.append(f"{symbol}: Net insider buying detected")

    # 2. Congress Trades (STOCK Act)
    cg_sig = congress.get("signal", "neutral")
    cg_trades = congress.get("trades") or []
    cg_buys = [t for t in cg_trades if "buy" in str(t.get("transaction_type", "")).lower()]
    cg_sells = [t for t in cg_trades if "sell" in str(t.get("transaction_type", "")).lower()]
    if cg_sig == "bullish" or len(cg_buys) >= 2:
        score += 8.0
        bull_catalysts.append(f"{symbol}: Congress members disclosed purchases under STOCK Act")
    elif cg_sig == "bearish" or (len(cg_sells) >= 3 and not cg_buys):
        score -= 7.0
        bear_catalysts.append(f"{symbol}: Congress members disclosed net sales under STOCK Act")

    # 3. Seasonality (Current Calendar Month)
    current_month_num = datetime.now(timezone.utc).month
    months = seasonality.get("months") or []
    cur_month_data = next((m for m in months if m.get("month_num") == current_month_num), None)
    if cur_month_data:
        avg_ret = cur_month_data.get("avg_return_pct", 0)
        m_name = cur_month_data.get("month", "")
        if avg_ret >= 2.5:
            score += 10.0
            bull_catalysts.append(f"{symbol}: Enters strong seasonal month ({m_name} avg {avg_ret:+.1f}%)")
        elif avg_ret >= 1.0:
            score += 5.0
            bull_catalysts.append(f"{symbol}: Positive seasonal historical return ({m_name} avg {avg_ret:+.1f}%)")
        elif avg_ret <= -2.5:
            score -= 10.0
            bear_catalysts.append(f"{symbol}: Historically weak seasonal month ({m_name} avg {avg_ret:+.1f}%)")
        elif avg_ret <= -1.0:
            score -= 5.0
            bear_catalysts.append(f"{symbol}: Negative seasonal historical return ({m_name} avg {avg_ret:+.1f}%)")

    # 4. Corporate Actions (8-K)
    act_list = actions.get("actions") or []
    for a in act_list[:5]:
        a_type = a.get("type", "")
        if a_type in ("Earnings Results", "Dividend"):
            score += 2.0
        elif a_type in ("Executive Changes", "Termination of Agreement"):
            score -= 3.0
            bear_catalysts.append(f"{symbol}: Recent 8-K disclosure ({a_type})")

    # 5. Ownership
    owners = ownership.get("owners") or []
    if len(owners) >= 5:
        score += 3.0

    final_score = int(max(10, min(95, score)))
    composite = "bullish" if final_score >= 58 else ("bearish" if final_score <= 42 else "neutral")

    return {
        "symbol": symbol,
        "intelligence_score": final_score,
        "composite_signal": composite,
        "bull_catalysts": bull_catalysts,
        "bear_catalysts": bear_catalysts,
        "insider_summary": insider.get("summary", ""),
        "congress_summary": congress.get("summary", ""),
        "seasonality_summary": seasonality.get("summary", ""),
        "corporate_actions_summary": actions.get("summary", ""),
        "ownership_summary": ownership.get("summary", ""),
    }

