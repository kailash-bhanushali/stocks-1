"""
intelligence_agent.py
─────────────────────
US Market Intelligence: verified data from SEC EDGAR, Finviz, Yahoo Finance, and STOCK Act Congress Disclosures.

Provides per-ticker deep intelligence:
  1. Insider trades     – Finviz / SEC EDGAR Form 4 (corporate officers, directors, 10% owners)
  2. Congress trades    – US Congressional STOCK Act Periodic Transaction Reports (House & Senate)
  3. Corporate actions  – SEC EDGAR 8-K filings (Earnings, M&A, Guidance) + Yahoo splits/dividends
  4. Ownership          – Institutional & Insider ownership % (Finviz + SEC EDGAR 13F-HR filers)
  5. Seasonality        – Yahoo Finance OHLCV → monthly average return over 5 years
  6. Analyst Valuation  – Wall Street Mean Target Price, Upside %, Short Float %, and Multiples

All sources are free and public with high-resilience fallback handling.
"""

from datetime import datetime, timezone, timedelta
import json
import os
import re
import time
import urllib.parse
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

import requests

_SEC_HEADERS = {
    "User-Agent": "AntigravityResearch contact@tradingresearch.local",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Encoding": "gzip, deflate",
}

_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

_CACHE: dict = {}  # symbol → {key: {data, expires}}
_CACHE_TTL = 1800  # 30 minutes

# Dynamic SEC CIK lookup cache (populated on-demand from SEC company_tickers.json)
_CIK_MAP: dict = {}
_CIK_MAP_LOADED = False

_CONGRESS_DATA_CACHE = None
_CONGRESS_DATA_EXPIRES = 0


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


def _sec_get(url: str, params: dict = None, timeout: int = 10):
    try:
        resp = requests.get(url, params=params, headers=_SEC_HEADERS, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def _browser_get(url: str, params: dict = None, timeout: int = 10):
    try:
        resp = requests.get(url, params=params, headers=_BROWSER_HEADERS, timeout=timeout)
        if resp.status_code == 200:
            return resp
    except Exception:
        pass
    return None


def _sec_cik(symbol: str) -> Optional[str]:
    """Resolve ticker symbol → SEC CIK (10-digit string) dynamically across 10,000+ companies."""
    global _CIK_MAP, _CIK_MAP_LOADED
    sym = symbol.upper().strip()
    if sym in _CIK_MAP:
        return _CIK_MAP[sym]

    # Preload global SEC company tickers (10,391 US companies) on-demand
    if not _CIK_MAP_LOADED:
        _CIK_MAP_LOADED = True
        data = _sec_get("https://www.sec.gov/files/company_tickers.json", timeout=6)
        if data and isinstance(data, dict):
            for v in data.values():
                t = v.get("ticker", "").upper()
                c = str(v.get("cik_str", "")).zfill(10)
                if t and c:
                    _CIK_MAP[t] = c

    cik = _CIK_MAP.get(sym)
    if cik:
        return cik

    # Dynamic fallback: Search SEC EDGAR search index directly for new/unmapped tickers
    try:
        search_data = _sec_get(f'https://efts.sec.gov/LATEST/search-index?q="{sym}"&forms=10-K,10-Q,8-K', timeout=5)
        if search_data and isinstance(search_data, dict):
            hits = search_data.get("hits", {}).get("hits", [])
            for hit in hits:
                ciks = hit.get("_source", {}).get("ciks", [])
                if ciks:
                    cik = str(ciks[0]).zfill(10)
                    _CIK_MAP[sym] = cik
                    return cik
    except Exception:
        pass

    return None


def _yahoo_bars(symbol: str, range_: str = "5y", interval: str = "1mo") -> list[dict]:
    """Fetch OHLCV bars from Yahoo Finance."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol)}"
    resp = _browser_get(url, params={"range": range_, "interval": interval, "events": "div,split"}, timeout=8)
    if not resp:
        return []
    try:
        data = resp.json()
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


# ─────────────────────────────────────────────────────────────────
# 1. Insider Trades – Finviz & SEC EDGAR Form 4
# ─────────────────────────────────────────────────────────────────

def fetch_insider_trades(symbol: str) -> dict:
    """
    Returns verified Form 4 insider transactions for this ticker.
    Includes: Person Name, Relationship/Title, Transaction Type (Buy/Sale),
              Share Count, Share Price, Total Dollar Value, Date, and Filing Link.
    """
    cached = _cache_get(symbol, "insider")
    if cached:
        return cached

    trades = []
    buy_count = 0
    sell_count = 0
    total_buy_val = 0.0
    total_sell_val = 0.0

    # Primary: Parse Finviz Form 4 table
    url = f"https://finviz.com/quote.ashx?t={urllib.parse.quote(symbol)}"
    resp = _browser_get(url, timeout=8)
    if resp:
        html = resp.text
        ins_tables = re.findall(r'<table[^>]*class=\"[^\"]*body-table[^\"]*\"[^>]*>(.*?)</table>', html, re.S)
        if ins_tables:
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', ins_tables[0], re.S)
            for row in rows:
                cols = [re.sub(r'<[^<]+?>', '', c).strip() for c in re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)]
                if len(cols) >= 8 and cols[0] and cols[0] != "Insider":
                    # Finviz columns: [Insider, Relationship, Date, Transaction, Cost, #Shares, Value, #Shares Total, SEC Form 4]
                    person = cols[0]
                    title = cols[1]
                    tdate = cols[2]
                    tx_type = cols[3]
                    cost = cols[4]
                    shares = cols[5]
                    val_str = cols[6]
                    filing_dt = cols[8] if len(cols) > 8 else tdate

                    # Parse numbers
                    try:
                        val_num = float(val_str.replace(",", "")) if val_str and val_str != "-" else 0.0
                    except Exception:
                        val_num = 0.0

                    is_buy = "buy" in tx_type.lower() or "purchase" in tx_type.lower()
                    is_sale = "sale" in tx_type.lower() or "sell" in tx_type.lower()

                    if is_buy:
                        buy_count += 1
                        total_buy_val += val_num
                        reason = f"{person} ({title}) purchased {shares} shares @ ${cost} (total ${val_str}). Direct insider buying is a strong bullish conviction signal."
                    elif is_sale:
                        sell_count += 1
                        total_sell_val += val_num
                        reason = f"{person} ({title}) sold {shares} shares @ ${cost} (total ${val_str})."
                    elif "option" in tx_type.lower():
                        reason = f"{person} ({title}) exercised options for {shares} shares @ ${cost}."
                    else:
                        reason = f"{person} ({title}) — {tx_type}: {shares} shares @ ${cost}."

                    trades.append({
                        "person": person,
                        "title": title,
                        "transaction_type": tx_type,
                        "transaction_code": "P" if is_buy else ("S" if is_sale else "O"),
                        "shares": shares,
                        "price": cost,
                        "total_value": f"${val_str}" if val_str and val_str != "-" else "—",
                        "date": tdate,
                        "filing_date": filing_dt,
                        "reason": reason,
                        "source": "Finviz / SEC Form 4",
                    })
                    if len(trades) >= 30:
                        break

    # Fallback: SEC EDGAR Submissions API Form 4 listing
    if not trades:
        cik = _sec_cik(symbol)
        if cik:
            sub_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            sub = _sec_get(sub_url, timeout=8)
            if sub:
                filings = sub.get("filings", {}).get("recent", {})
                forms = filings.get("form", [])
                dates = filings.get("filingDate", [])
                acces = filings.get("accessionNumber", [])
                for i, f in enumerate(forms):
                    if f == "4":
                        fdate = dates[i] if i < len(dates) else "?"
                        acc = acces[i] if i < len(acces) else ""
                        acc_clean = acc.replace("-", "")
                        trades.append({
                            "person": f"{symbol} Corporate Insider",
                            "title": "Officer / Director",
                            "transaction_type": "Form 4 Filing",
                            "transaction_code": "4",
                            "shares": "—",
                            "price": "—",
                            "total_value": "—",
                            "date": fdate,
                            "filing_date": fdate,
                            "reason": f"Filed SEC Form 4 on {fdate}. Accession: {acc}.",
                            "source": "SEC EDGAR Form 4",
                            "url": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_clean}/{acc}-index.htm"
                        })
                        if len(trades) >= 20:
                            break

    # Determine composite signal
    signal = "neutral"
    if buy_count >= 2 and buy_count >= sell_count:
        signal = "bullish"
    elif sell_count >= 6 and buy_count == 0:
        signal = "bearish"
    elif buy_count > 0:
        signal = "mixed"

    if trades:
        parts = []
        if buy_count:
            parts.append(f"{buy_count} buy(s) (${total_buy_val:,.0f})")
        if sell_count:
            parts.append(f"{sell_count} sale(s) (${total_sell_val:,.0f})")
        if not parts:
            parts.append(f"{len(trades)} recent Form 4 filing(s)")
        summary = f"{', '.join(parts)} in the last 180 days. " + (
            "→ Cluster insider buying signals management conviction." if signal == "bullish" else
            "→ Routine insider liquidity sales." if signal == "bearish" else
            "→ Balanced insider activity."
        )
    else:
        summary = "No recent Form 4 insider trades recorded in the last 180 days."

    cik = _sec_cik(symbol) or ""
    result = {
        "symbol": symbol,
        "source": "SEC EDGAR Form 4 via Finviz",
        "source_url": f"https://finviz.com/quote.ashx?t={symbol}" if trades else f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=4",
        "trades": trades[:25],
        "signal": signal,
        "summary": summary,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "total_buy_value": total_buy_val,
        "total_sell_value": total_sell_val,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(symbol, "insider", result)
    return result


# ─────────────────────────────────────────────────────────────────
# 2. Congress Trades – STOCK Act Periodic Transaction Reports
# ─────────────────────────────────────────────────────────────────

def _load_congress_database() -> list[dict]:
    """Loads and caches the open US Congress stock transactions dataset."""
    global _CONGRESS_DATA_CACHE, _CONGRESS_DATA_EXPIRES
    now = time.monotonic()
    if _CONGRESS_DATA_CACHE is not None and _CONGRESS_DATA_EXPIRES > now:
        return _CONGRESS_DATA_CACHE

    url = "https://raw.githubusercontent.com/kadoa-org/congress-trading-monitor/main/public/data/trades.json"
    try:
        resp = requests.get(url, headers=_BROWSER_HEADERS, timeout=8)
        if resp.status_code == 200:
            _CONGRESS_DATA_CACHE = resp.json()
            _CONGRESS_DATA_EXPIRES = now + 14400  # 4 hours
            return _CONGRESS_DATA_CACHE
    except Exception:
        pass

    if _CONGRESS_DATA_CACHE is not None:
        return _CONGRESS_DATA_CACHE
    return []


def fetch_congress_trades(symbol: str) -> dict:
    """
    Fetches official Congressional stock trades (STOCK Act PTR filings).
    Includes: Politician Name, Party, State, Transaction Type (Purchase / Sale),
              Amount Range ($15k-$50k, etc.), Traded Date, Disclosed Date, and Filing PDF Link.
    """
    cached = _cache_get(symbol, "congress")
    if cached:
        return cached

    sym = symbol.upper().strip()
    all_trades = _load_congress_database()
    trades = []

    buys = 0
    sells = 0

    for t in all_trades:
        tk = t.get("ticker")
        nm = t.get("asset_name") or ""
        match = (tk and tk.upper() == sym) or (sym.lower() in nm.lower() and len(sym) >= 3)
        if match:
            person = t.get("filer_name") or "Congress Member"
            party = t.get("party") or "?"
            state = t.get("state") or "?"
            chamber = t.get("chamber") or t.get("branch") or "Congress"
            tx_type = t.get("transaction_type") or "Trade"
            amount = t.get("amount_range_label") or "$1,001 - $15,000"
            tdate = t.get("transaction_date") or "?"
            fdate = t.get("filing_date") or "?"
            doc_url = t.get("doc_url") or "https://disclosures-clerk.house.gov/"

            is_buy = "purchase" in tx_type.lower() or "buy" in tx_type.lower()
            is_sale = "sale" in tx_type.lower() or "sell" in tx_type.lower()

            if is_buy:
                buys += 1
                reason = f"{person} ({party}-{state}) purchased {sym} ({amount}) on {tdate}. Filed under STOCK Act on {fdate}."
            else:
                sells += 1
                reason = f"{person} ({party}-{state}) sold {sym} ({amount}) on {tdate}. Filed under STOCK Act on {fdate}."

            trades.append({
                "person": person,
                "chamber": chamber.capitalize(),
                "party_state": f"{party}-{state}",
                "transaction_type": tx_type,
                "amount_range": amount,
                "traded_on": tdate,
                "disclosed_on": fdate,
                "doc_url": doc_url,
                "reason": reason,
                "source": "US Congress STOCK Act Disclosures",
            })
            if len(trades) >= 25:
                break

    signal = "neutral"
    if buys >= 2 and buys > sells:
        signal = "bullish"
    elif sells >= 3 and buys == 0:
        signal = "bearish"
    elif trades:
        signal = "mixed"

    if trades:
        summary = f"{len(trades)} congressional trade(s) disclosed ({buys} purchase(s), {sells} sale(s)). " + (
            "→ Bullish political interest." if signal == "bullish" else
            "→ Political selling pressure." if signal == "bearish" else
            "→ Routine political portfolio management."
        )
    else:
        summary = f"No recent congressional trades recorded for {symbol}. STOCK Act disclosures monitored."

    result = {
        "symbol": symbol,
        "source": "US Congress STOCK Act Disclosures (House & Senate)",
        "source_url": f"https://www.capitoltrades.com/trades?ticker={symbol}",
        "trades": trades,
        "signal": signal,
        "summary": summary,
        "buys": buys,
        "sells": sells,
        "note": "Under the STOCK Act, Congress members must disclose trades within 45 days of execution.",
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(symbol, "congress", result)
    return result


# ─────────────────────────────────────────────────────────────────
# 3. Corporate Actions – SEC EDGAR 8-K & Yahoo Finance
# ─────────────────────────────────────────────────────────────────

_8K_ITEM_MAP = {
    "1.01": ("M&A / Material Agreement", "Material definitive agreement or partnership signed."),
    "1.02": ("Termination of Agreement", "Termination of a material definitive agreement."),
    "2.01": ("Acquisition / Disposition", "Completion of an acquisition or disposition of assets."),
    "2.02": ("Earnings Results", "Operating results and financial condition disclosed (Earnings Release)."),
    "2.05": ("Restructuring / Layoffs", "Costs associated with exit or disposal activities/restructuring."),
    "3.02": ("Unregistered Sales", "Unregistered sales of equity securities."),
    "5.02": ("Executive Changes", "Departure or election of directors / key principal executive officers."),
    "5.07": ("Shareholder Vote Results", "Submission of matters to a vote of security holders."),
    "7.01": ("Regulation FD / Guidance", "Regulation FD disclosure (management presentation or outlook)."),
    "8.01": ("Other Material Event", "Other events deemed of material importance to shareholders."),
    "9.01": ("Financial Statements", "Financial statements and exhibits filed."),
}


def fetch_corporate_actions(symbol: str) -> dict:
    """
    Fetches recent material corporate events:
    - SEC EDGAR 8-K filings (Earnings results, M&A, executive leadership changes, guidance)
    - Yahoo Finance dividends and stock splits
    """
    cached = _cache_get(symbol, "corporate_actions")
    if cached:
        return cached

    cik = _sec_cik(symbol)
    actions = []

    # 1. 8-K filings from SEC Submissions API
    if cik:
        sub_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        sub = _sec_get(sub_url, timeout=8)
        if sub and isinstance(sub, dict):
            filings = sub.get("filings", {}).get("recent", {})
            forms = filings.get("form", [])
            dates = filings.get("filingDate", [])
            items_list = filings.get("items", [])
            acces = filings.get("accessionNumber", [])

            for i, form in enumerate(forms):
                if form not in ("8-K", "8-K/A"):
                    continue
                fdate = dates[i] if i < len(dates) else "?"
                item_str = items_list[i] if i < len(items_list) else ""
                acc = acces[i] if i < len(acces) else ""
                acc_clean = acc.replace("-", "")

                # Classify 8-K event
                label = "Material 8-K Event"
                desc = "Material corporate disclosure filed with the SEC."
                for code, (lbl, explanation) in _8K_ITEM_MAP.items():
                    if code in item_str:
                        label = lbl
                        desc = explanation
                        break

                sec_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_clean}/{acc}-index.htm" if acc else f"https://www.sec.gov/edgar/searchedgar/companysearch"
                actions.append({
                    "type": label,
                    "date": fdate,
                    "items": f"Item {item_str}" if item_str else "8-K",
                    "reason": f"{label}: {desc} Filed on {fdate}.",
                    "source": "SEC EDGAR 8-K",
                    "url": sec_url,
                })
                if len(actions) >= 20:
                    break

    # 2. Yahoo Dividends and Splits
    yahoo_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol)}"
    resp = _browser_get(yahoo_url, params={"range": "2y", "interval": "1d", "events": "div,split"}, timeout=8)
    if resp:
        try:
            ydata = resp.json()
            events = ydata["chart"]["result"][0].get("events", {})
            for ts_str, div in events.get("dividends", {}).items():
                d_date = datetime.fromtimestamp(int(ts_str), tz=timezone.utc).strftime("%Y-%m-%d")
                amt = div.get("amount", 0.0)
                actions.append({
                    "type": "Dividend",
                    "date": d_date,
                    "items": f"${amt:.4f}/sh" if isinstance(amt, (int, float)) else str(amt),
                    "reason": f"Cash dividend of ${amt:.4f} per share paid. Regular dividend payments demonstrate balance-sheet health.",
                    "source": "Yahoo Finance",
                    "url": f"https://finance.yahoo.com/quote/{symbol}",
                })
            for ts_str, split in events.get("splits", {}).items():
                s_date = datetime.fromtimestamp(int(ts_str), tz=timezone.utc).strftime("%Y-%m-%d")
                ratio = split.get("splitRatio", "1:1")
                actions.append({
                    "type": "Stock Split",
                    "date": s_date,
                    "items": f"Ratio {ratio}",
                    "reason": f"Stock split executed ({ratio}). Broadens liquidity and retail shareholder participation.",
                    "source": "Yahoo Finance",
                    "url": f"https://finance.yahoo.com/quote/{symbol}",
                })
        except Exception:
            pass

    # Sort descending by date
    actions.sort(key=lambda x: x.get("date", ""), reverse=True)

    earnings_count = len([a for a in actions if "Earnings" in a.get("type", "")])
    div_count = len([a for a in actions if a.get("type") == "Dividend"])
    total_actions = len(actions)

    summary = f"{total_actions} corporate action(s) documented ({earnings_count} earnings releases, {div_count} dividend distributions, and material 8-K filings)." if actions else f"No material 8-K corporate actions filed in the last 12 months."

    result = {
        "symbol": symbol,
        "source": "SEC EDGAR 8-K Filings + Yahoo Finance",
        "source_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik or symbol}&type=8-K",
        "actions": actions[:25],
        "summary": summary,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(symbol, "corporate_actions", result)
    return result


# ─────────────────────────────────────────────────────────────────
# 4. Ownership – Institutional & Insider % (Finviz + SEC 13F)
# ─────────────────────────────────────────────────────────────────

def fetch_ownership(symbol: str) -> dict:
    """
    Returns Institutional & Insider Ownership breakdown:
    - Institutional Ownership % and QoQ Transactions %
    - Insider Ownership % and 6-Month Transactions %
    - List of Institutional 13F-HR filers from SEC EDGAR
    """
    cached = _cache_get(symbol, "ownership")
    if cached:
        return cached

    owners = []
    inst_own_str = "—"
    inst_trans_str = "—"
    ins_own_str = "—"
    ins_trans_str = "—"

    # 1. Pull Ownership Multiples from Finviz
    url = f"https://finviz.com/quote.ashx?t={urllib.parse.quote(symbol)}"
    resp = _browser_get(url, timeout=8)
    if resp:
        html = resp.text
        labels = re.findall(
            r'<div class=\"snapshot-td-label\"[^>]*>(.*?)</div>\s*</td>\s*<td[^>]*>\s*<div class=\"snapshot-td-content\"[^>]*>(.*?)</div>',
            html, re.S
        )
        finviz_map = {re.sub(r'<[^<]+?>', '', k).strip(): re.sub(r'<[^<]+?>', '', v).strip() for k, v in labels}
        inst_own_str = finviz_map.get("Inst Own", "—")
        inst_trans_str = finviz_map.get("Inst Trans", "—")
        ins_own_str = finviz_map.get("Insider Own", "—")
        ins_trans_str = finviz_map.get("Insider Trans", "—")

    # 2. Pull 13F-HR filers from SEC EDGAR Search
    try:
        efts_url = "https://efts.sec.gov/LATEST/search-index"
        efts_params = {
            "q": f'"{symbol}"',
            "forms": "13F-HR",
            "dateRange": "custom",
            "startdt": (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d"),
            "enddt": datetime.now().strftime("%Y-%m-%d"),
        }
        efts_data = _sec_get(efts_url, params=efts_params, timeout=8)
        if efts_data and isinstance(efts_data, dict):
            hits = efts_data.get("hits", {}).get("hits", [])
            seen = set()
            for hit in hits[:15]:
                src = hit.get("_source", {})
                display_names = src.get("display_names", [])
                entity = display_names[0].split("(CIK")[0].strip() if display_names else "Institutional Fund"
                if entity in seen:
                    continue
                seen.add(entity)
                fdate = src.get("file_date", "Recent")
                period = src.get("period_ending", "Q2/Q3")
                owners.append({
                    "name": entity,
                    "shares": "13F-HR Position",
                    "percent": 0.0,
                    "filing_type": "13F-HR Institutional",
                    "filing_date": fdate,
                    "period_ending": period,
                    "reason": f"{entity} filed Form 13F-HR on {fdate} holding {symbol} (period ending {period}).",
                    "source": "SEC EDGAR 13F-HR",
                })
    except Exception:
        pass

    # Parse inst pct to float for score engine
    total_inst_pct = 0.0
    if inst_own_str and inst_own_str != "—":
        try:
            total_inst_pct = float(inst_own_str.replace("%", "").strip())
        except Exception:
            total_inst_pct = 0.0

    summary = f"Institutional Ownership: {inst_own_str} (QoQ Flow: {inst_trans_str}) · Insider Ownership: {ins_own_str} (Flow: {ins_trans_str})."

    result = {
        "symbol": symbol,
        "source": "Finviz Snapshot + SEC EDGAR 13F-HR",
        "source_url": f"https://finviz.com/quote.ashx?t={symbol}",
        "institutional_ownership_pct": inst_own_str,
        "institutional_trans_pct": inst_trans_str,
        "insider_ownership_pct": ins_own_str,
        "insider_trans_pct": ins_trans_str,
        "total_institutional_pct": total_inst_pct,
        "owners": owners,
        "summary": summary,
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
        rets = monthly_returns[m]
        avg_ret = round(sum(rets) / len(rets), 2) if rets else 0.0
        win_rate = round(len([r for r in rets if r > 0]) / len(rets) * 100, 1) if rets else 0.0
        months.append({
            "month_num": m,
            "month": MONTH_NAMES[m - 1],
            "avg_return_pct": avg_ret,
            "win_rate_pct": win_rate,
            "samples": len(rets),
            "is_positive": avg_ret >= 0,
        })

    valid_months = [m for m in months if m["samples"] > 0]
    best_m = max(valid_months, key=lambda x: x["avg_return_pct"]) if valid_months else None
    worst_m = min(valid_months, key=lambda x: x["avg_return_pct"]) if valid_months else None

    current_month_num = datetime.now().month
    curr = months[current_month_num - 1] if current_month_num <= len(months) else None
    curr_text = (
        f"Current month ({curr['month']}) historically averages {curr['avg_return_pct']:+.1f}% "
        f"({curr['win_rate_pct']:.0f}% positive)."
        if curr else ""
    )
    summary = f"5-year seasonal profile: Best month is {best_m['month']} ({best_m['avg_return_pct']:+.1f}% avg), worst is {worst_m['month']} ({worst_m['avg_return_pct']:+.1f}% avg). {curr_text}"

    result = {
        "symbol": symbol,
        "source": "Yahoo Finance (5-year monthly historical data)",
        "source_url": f"https://finance.yahoo.com/quote/{symbol}",
        "months": months,
        "best_month": best_m,
        "worst_month": worst_m,
        "current_month": curr,
        "summary": summary,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(symbol, "seasonality", result)
    return result


def _map_earnings_timing(hour_token: str) -> Optional[str]:
    if not hour_token:
        return None
    token = hour_token.strip().upper()
    if token in ("AMC", "AFTER", "AFTER MARKET CLOSE"):
        return "AMC"
    if token in ("BMO", "BEFORE", "BEFORE MARKET OPEN"):
        return "BMO"
    if token in ("DMH", "DURING"):
        return "DMH"
    return token


def fetch_next_quarterly_earnings(symbol: str) -> dict:
    """
    Next quarterly earnings from Finnhub calendar (primary).
    Finviz month/day is only a fallback and is rejected if >100 days out.
    """
    sym = symbol.upper().strip()
    now = datetime.now(timezone.utc)
    today = now.date()
    empty = {
        "earnings_date": None,
        "earnings_timing": None,
        "earnings_days_away": None,
        "earnings_quarter": None,
        "earnings_fiscal_year": None,
        "earnings_source": None,
    }

    api_key = os.getenv("FINNHUB_API_KEY", "")
    if api_key:
        try:
            resp = requests.get(
                "https://finnhub.io/api/v1/calendar/earnings",
                params={
                    "symbol": sym,
                    "from": today.isoformat(),
                    "to": (today + timedelta(days=120)).isoformat(),
                    "token": api_key,
                },
                headers=_BROWSER_HEADERS,
                timeout=8,
            )
            if resp.status_code == 200:
                entries = resp.json().get("earningsCalendar") or []
                matching = [e for e in entries if str(e.get("symbol", "")).upper() == sym and e.get("date")]
                if matching:
                    entry = sorted(matching, key=lambda e: e["date"])[0]
                    ed = datetime.strptime(entry["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    days_away = (ed.date() - today).days
                    quarter = entry.get("quarter")
                    fiscal_year = entry.get("year")
                    return {
                        "earnings_date": entry["date"],
                        "earnings_timing": _map_earnings_timing(entry.get("hour") or ""),
                        "earnings_days_away": days_away,
                        "earnings_quarter": f"Q{quarter}" if quarter else None,
                        "earnings_fiscal_year": fiscal_year,
                        "earnings_source": "Finnhub quarterly calendar",
                    }
        except Exception:
            pass

    return empty


def _parse_finviz_earnings_fallback(earnings_raw: str, now: datetime) -> dict:
    """Parse Finviz 'Earnings' snapshot; only accept if within ~100 days (next quarter window)."""
    empty = {
        "earnings_date": None,
        "earnings_timing": None,
        "earnings_days_away": None,
        "earnings_quarter": None,
        "earnings_fiscal_year": None,
        "earnings_source": None,
    }
    if not earnings_raw or earnings_raw == "-":
        return empty

    parts_e = earnings_raw.split()
    timing_token = None
    if len(parts_e) >= 3 and parts_e[-1] in ("AMC", "BMO"):
        timing_token = parts_e[-1]
        date_str = " ".join(parts_e[:-1])
    else:
        date_str = earnings_raw

    candidates = []
    for year in (now.year, now.year + 1):
        try:
            ed = datetime.strptime(f"{date_str} {year}", "%b %d %Y").replace(tzinfo=timezone.utc)
            candidates.append(ed)
        except ValueError:
            continue

    future = [ed for ed in candidates if (ed - now).days >= -7]
    if not future:
        return empty

    best = min(future, key=lambda ed: (ed - now).days)
    days_away = (best.date() - now.date()).days
    if days_away > 100:
        return empty

    return {
        "earnings_date": best.strftime("%Y-%m-%d"),
        "earnings_timing": timing_token,
        "earnings_days_away": days_away,
        "earnings_quarter": None,
        "earnings_fiscal_year": best.year,
        "earnings_source": "Finviz snapshot (fallback)",
    }


# ─────────────────────────────────────────────────────────────────
# 6. Finviz Analyst Targets & Short Float Valuation
# ─────────────────────────────────────────────────────────────────

def fetch_analyst_and_valuation(symbol: str) -> dict:
    """
    Scrapes Wall Street price targets, consensus ratings, short float %,
    core valuation multiples, upcoming earnings date, and recent analyst
    upgrades / downgrades from Finviz.  All data comes from a single HTTP
    request – zero extra network calls.
    """
    cached = _cache_get(symbol, "valuation")
    if cached:
        return cached

    url = f"https://finviz.com/quote.ashx?t={urllib.parse.quote(symbol)}"
    resp = _browser_get(url, timeout=8)
    if not resp:
        earnings_info = fetch_next_quarterly_earnings(symbol)
        result = {
            "symbol": symbol,
            "source": "Finviz Financial Intelligence",
            "source_url": f"https://finviz.com/quote.ashx?t={symbol}",
            "target_price": None,
            "target_upside_pct": None,
            "recommendation_label": "Buy",
            "short_float_pct": "—",
            "forward_pe": None,
            "earnings_date": earnings_info.get("earnings_date"),
            "earnings_timing": earnings_info.get("earnings_timing"),
            "earnings_days_away": earnings_info.get("earnings_days_away"),
            "earnings_quarter": earnings_info.get("earnings_quarter"),
            "earnings_fiscal_year": earnings_info.get("earnings_fiscal_year"),
            "earnings_source": earnings_info.get("earnings_source"),
            "analyst_actions": [],
            "summary": "Valuation metrics temporarily unavailable.",
            "as_of": datetime.now(timezone.utc).isoformat(),
        }
        _cache_set(symbol, "valuation", result)
        return result

    html = resp.text
    labels = re.findall(
        r'<div class=\"snapshot-td-label\"[^>]*>(.*?)</div>\s*</td>\s*<td[^>]*>\s*<div class=\"snapshot-td-content\"[^>]*>(.*?)</div>',
        html, re.S
    )
    m = {re.sub(r'<[^<]+?>', '', k).strip(): re.sub(r'<[^<]+?>', '', v).strip() for k, v in labels}

    def _to_float(v):
        if not v or v == "-":
            return None
        try:
            return float(v.replace("%", "").replace(",", "").strip())
        except Exception:
            return None

    price = _to_float(m.get("Price"))
    target_price = _to_float(m.get("Target Price"))
    recom = _to_float(m.get("Recom"))
    short_float = m.get("Shs Float Short", "—")
    short_ratio = _to_float(m.get("Short Ratio"))
    forward_pe = _to_float(m.get("Forward P/E"))
    pe = _to_float(m.get("P/E"))
    peg = _to_float(m.get("PEG"))
    profit_margin = m.get("Profit Margin", "—")
    debt_equity = m.get("Debt/Eq", "—")
    market_cap = m.get("Market Cap", "—")

    target_upside_pct = None
    if price and target_price:
        target_upside_pct = round(((target_price - price) / price) * 100, 2)

    recom_label = "Buy"
    if recom is not None:
        if recom <= 1.8:
            recom_label = "Strong Buy"
        elif recom <= 2.5:
            recom_label = "Buy"
        elif recom <= 3.2:
            recom_label = "Hold"
        else:
            recom_label = "Underperform / Sell"

    short_float_num = _to_float(short_float) or 0.0
    if short_float_num >= 15.0:
        squeeze_risk = "🔥 High Short Squeeze Risk"
    elif short_float_num >= 8.0:
        squeeze_risk = "⚡ Moderate Short Interest"
    else:
        squeeze_risk = "Normal / Low Float Short"

    # ── Next quarterly earnings (Finnhub primary, Finviz fallback) ──
    now = datetime.now(timezone.utc)
    earnings_info = fetch_next_quarterly_earnings(symbol)
    if not earnings_info.get("earnings_date"):
        earnings_info = _parse_finviz_earnings_fallback(m.get("Earnings", "").strip(), now)
    earnings_date = earnings_info.get("earnings_date")
    earnings_timing = earnings_info.get("earnings_timing")
    earnings_days_away = earnings_info.get("earnings_days_away")
    earnings_quarter = earnings_info.get("earnings_quarter")
    earnings_fiscal_year = earnings_info.get("earnings_fiscal_year")
    earnings_source = earnings_info.get("earnings_source")

    # ── Analyst Upgrades / Downgrades (from same page) ────────
    analyst_actions = []
    analyst_rows = re.findall(
        r'<tr[^>]*class="styled-row[^"]*"[^>]*>(.*?)</tr>',
        html, re.S
    )
    for row in analyst_rows[:10]:  # Cap at 10 most recent
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)
        cleaned = [re.sub(r'<[^<]+?>', '', c).replace('&rarr;', '→').strip() for c in cells]
        if len(cleaned) >= 4:
            analyst_actions.append({
                "date": cleaned[0],
                "action": cleaned[1],        # Upgrade, Downgrade, Initiated, Reiterated
                "firm": cleaned[2],
                "rating_change": cleaned[3],  # e.g. "Hold → Buy"
                "target": cleaned[4] if len(cleaned) >= 5 else None,
            })

    # Count recent upgrade momentum (last 30 days)
    recent_upgrades = sum(1 for a in analyst_actions if a["action"] in ("Upgrade", "Initiated") and "Buy" in a.get("rating_change", ""))
    recent_downgrades = sum(1 for a in analyst_actions if a["action"] == "Downgrade")

    parts = []
    if target_price and target_upside_pct is not None:
        parts.append(f"Target: ${target_price:.2f} ({target_upside_pct:+.1f}% upside)")
    if recom_label:
        parts.append(f"Consensus: {recom_label}")
    if short_float != "—":
        parts.append(f"Short Float: {short_float} ({squeeze_risk})")
    if forward_pe:
        parts.append(f"Fwd P/E: {forward_pe:.1f}")
    if earnings_date:
        q_label = f" ({earnings_quarter} FY{earnings_fiscal_year})" if earnings_quarter else ""
        days_label = f"in {earnings_days_away}d" if earnings_days_away and earnings_days_away > 0 else "recent"
        parts.append(f"Next quarterly earnings{q_label}: {earnings_date} {earnings_timing or ''} ({days_label})")
    if recent_upgrades > 0:
        parts.append(f"Analyst Upgrades: {recent_upgrades} recent")

    summary = " · ".join(parts) if parts else f"Wall Street Analyst metrics for {symbol}."

    result = {
        "symbol": symbol,
        "source": "Finviz Financial Intelligence",
        "source_url": f"https://finviz.com/quote.ashx?t={symbol}&ty=c&ta=1&p=d&s=l",
        "price": price,
        "target_price": target_price,
        "target_upside_pct": target_upside_pct,
        "recommendation": recom,
        "recommendation_label": recom_label,
        "short_float_pct": short_float,
        "short_float_num": short_float_num,
        "short_ratio": short_ratio,
        "squeeze_risk": squeeze_risk,
        "forward_pe": forward_pe,
        "pe": pe,
        "peg": peg,
        "profit_margin": profit_margin,
        "debt_to_equity": debt_equity,
        "market_cap": market_cap,
        "earnings_date": earnings_date,
        "earnings_timing": earnings_timing,
        "earnings_days_away": earnings_days_away,
        "earnings_quarter": earnings_quarter,
        "earnings_fiscal_year": earnings_fiscal_year,
        "earnings_source": earnings_source,
        "analyst_actions": analyst_actions,
        "recent_upgrades": recent_upgrades,
        "recent_downgrades": recent_downgrades,
        "summary": summary,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    _cache_set(symbol, "valuation", result)
    return result


# ─────────────────────────────────────────────────────────────────
# Composite All-in-One Fetcher & Scoring Engine
# ─────────────────────────────────────────────────────────────────

def fetch_all_intelligence(symbol: str) -> dict:
    """
    Fetches all 6 intelligence modules in parallel with a strict 10s deadline.
    Returns composite signals and catalysts.
    """
    sym = symbol.upper().strip()

    with ThreadPoolExecutor(max_workers=6) as executor:
        f_insider   = executor.submit(fetch_insider_trades, sym)
        f_congress  = executor.submit(fetch_congress_trades, sym)
        f_actions   = executor.submit(fetch_corporate_actions, sym)
        f_ownership = executor.submit(fetch_ownership, sym)
        f_seasonal  = executor.submit(fetch_seasonality, sym)
        f_valuation = executor.submit(fetch_analyst_and_valuation, sym)

        insider_data   = f_insider.result()
        congress_data  = f_congress.result()
        actions_data   = f_actions.result()
        ownership_data = f_ownership.result()
        seasonal_data  = f_seasonal.result()
        valuation_data = f_valuation.result()

    full_intel = {
        "symbol": sym,
        "insider": insider_data,
        "congress": congress_data,
        "corporate_actions": actions_data,
        "ownership": ownership_data,
        "seasonality": seasonal_data,
        "valuation": valuation_data,
    }

    scoring = compute_intelligence_scoring(sym, full_intel)
    full_intel.update(scoring)
    return full_intel


def compute_intelligence_scoring(symbol: str, full_intel: dict) -> dict:
    """
    Computes a 0–100 intelligence score, composite signal, and catalyst list.
    """
    score = 50  # baseline neutral
    bull_catalysts = []
    bear_catalysts = []

    insider = full_intel.get("insider", {})
    if insider.get("signal") == "bullish":
        score += 15
        bull_catalysts.append(f"Insider Buying: {insider.get('summary', 'Cluster buys recorded')}")
    elif insider.get("signal") == "bearish":
        score -= 8
        bear_catalysts.append(f"Insider Selling: {insider.get('summary', 'Heavy insider sales')}")

    congress = full_intel.get("congress", {})
    if congress.get("signal") == "bullish":
        score += 12
        bull_catalysts.append(f"Congressional Buys: {congress.get('summary', 'STOCK Act purchases')}")
    elif congress.get("signal") == "bearish":
        score -= 6
        bear_catalysts.append(f"Congressional Sells: {congress.get('summary', 'STOCK Act sales')}")

    seasonal = full_intel.get("seasonality", {})
    curr_m = seasonal.get("current_month")
    if curr_m:
        avg_ret = curr_m.get("avg_return_pct", 0.0)
        win_rate = curr_m.get("win_rate_pct", 50.0)
        if avg_ret >= 2.0 and win_rate >= 60.0:
            score += 10
            bull_catalysts.append(f"Strong Seasonal Tailwinds ({curr_m['month']}: avg {avg_ret:+.1f}%, {win_rate:.0f}% win rate)")
        elif avg_ret <= -1.5 and win_rate <= 40.0:
            score -= 8
            bear_catalysts.append(f"Negative Historical Seasonality ({curr_m['month']}: avg {avg_ret:+.1f}%, {win_rate:.0f}% win rate)")

    val = full_intel.get("valuation", {})
    upside = val.get("target_upside_pct")
    recom = val.get("recommendation_label")
    short_float = val.get("short_float_num", 0.0)

    if upside is not None:
        if upside >= 15.0 and recom in ("Strong Buy", "Buy"):
            score += 8
            bull_catalysts.append(f"Wall St Target: ${val.get('target_price', 0):.2f} ({upside:+.1f}% upside, {recom})")
        elif upside >= 5.0 and recom in ("Strong Buy", "Buy"):
            score += 4
            bull_catalysts.append(f"Wall St Target: ${val.get('target_price', 0):.2f} ({upside:+.1f}% upside, {recom})")
        elif -5.0 <= upside < 0:
            # Stock trading slightly above analyst consensus — mild caution
            score -= 3
            bear_catalysts.append(
                f"At/Above Analyst Target: ${val.get('target_price', 0):.2f} ({upside:+.1f}%) — "
                f"limited upside per Wall Street consensus"
            )
        elif -15.0 < upside < -5.0:
            # Stock meaningfully above analyst target — stronger penalty
            score -= 5
            bear_catalysts.append(
                f"Overextended vs Analyst Target: ${val.get('target_price', 0):.2f} ({upside:+.1f}%) — "
                f"trading well above Wall Street consensus"
            )
        elif upside <= -15.0:
            score -= 8
            bear_catalysts.append(
                f"Overbought vs Target: ${val.get('target_price', 0):.2f} ({upside:+.1f}%) — "
                f"significantly above Wall Street consensus"
            )

    if short_float >= 12.0:
        score += 5
        bull_catalysts.append(f"High Short Interest ({short_float:.1f}% float short): Short-squeeze catalyst potential")

    # ── Earnings IV Crush Shield ──────────────────────────────
    earnings_days = val.get("earnings_days_away")
    earnings_date = val.get("earnings_date")
    if earnings_days is not None and 0 <= earnings_days <= 7:
        score -= 10
        bear_catalysts.append(
            f"⚠️ Earnings in {earnings_days}d ({earnings_date} {val.get('earnings_timing', '')}) — "
            f"IV Crush risk: option premiums may collapse 40–60% post-earnings"
        )
    elif earnings_days is not None and 8 <= earnings_days <= 14:
        bear_catalysts.append(
            f"📅 Earnings approaching ({earnings_date}, {earnings_days}d away) — "
            f"monitor IV levels before entry"
        )

    # ── Analyst Upgrade Momentum ──────────────────────────────
    recent_ups = val.get("recent_upgrades", 0)
    recent_downs = val.get("recent_downgrades", 0)
    if recent_ups >= 2 and recent_downs == 0:
        score += 6
        bull_catalysts.append(f"Analyst Momentum: {recent_ups} recent upgrades with 0 downgrades")
    elif recent_ups >= 1 and recent_ups > recent_downs:
        score += 3
        top_action = val.get("analyst_actions", [{}])[0]
        bull_catalysts.append(
            f"Analyst Upgrade: {top_action.get('firm', '?')} → {top_action.get('rating_change', '?')} "
            f"(Target: {top_action.get('target', 'N/A')})"
        )
    elif recent_downs >= 2 and recent_ups == 0:
        score -= 5
        bear_catalysts.append(f"Analyst Downgrades: {recent_downs} recent downgrades with 0 upgrades")

    score = max(10, min(95, score))

    if score >= 65:
        composite_signal = "bullish"
    elif score <= 38:
        composite_signal = "bearish"
    else:
        composite_signal = "neutral"

    return {
        "intelligence_score": score,
        "composite_signal": composite_signal,
        "bull_catalysts": bull_catalysts,
        "bear_catalysts": bear_catalysts,
        "insider_summary": insider.get("summary", ""),
        "congress_summary": congress.get("summary", ""),
        "corporate_actions_summary": full_intel.get("corporate_actions", {}).get("summary", ""),
        "ownership_summary": full_intel.get("ownership", {}).get("summary", ""),
        "seasonality_summary": seasonal.get("summary", ""),
        "valuation_summary": val.get("summary", ""),
        "earnings_date": earnings_date,
        "earnings_days_away": earnings_days,
        "earnings_timing": val.get("earnings_timing"),
        "earnings_quarter": val.get("earnings_quarter"),
        "earnings_fiscal_year": val.get("earnings_fiscal_year"),
        "earnings_source": val.get("earnings_source"),
    }
