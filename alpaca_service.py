import os
from datetime import date, datetime, timezone
from urllib.parse import urlsplit, urlunsplit

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce


def _normalize_alpaca_base_url(base_url: str) -> str:
    if not base_url:
        return ""
    cleaned = base_url.strip().rstrip("/")
    parsed = urlsplit(cleaned)
    if parsed.path == "/v2":
        return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
    return cleaned


def _alpaca_client():
    api_key = os.getenv("ALPACA_API_KEY", "")
    secret_key = os.getenv("ALPACA_SECRET_KEY", "")
    paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"
    base_url = _normalize_alpaca_base_url(os.getenv("ALPACA_BASE_URL", ""))
    if not api_key or not secret_key:
        raise RuntimeError("ALPACA_API_KEY and ALPACA_SECRET_KEY are required for live order submission.")
    return TradingClient(api_key, secret_key, paper=paper, url_override=base_url or None)


def _parse_occ_dte(symbol: str):
    import re
    match = re.match(r"^([A-Z]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d+)$", symbol or "")
    if not match:
        return None, None
    year = 2000 + int(match.group(2))
    month = int(match.group(3))
    day = int(match.group(4))
    try:
        exp = date(year, month, day)
        return match.group(1), (exp - date.today()).days
    except ValueError:
        return match.group(1), None


def alpaca_account_summary():
    account = _alpaca_client().get_account()
    positions = []
    try:
        positions = get_positions()
        option_positions = [p for p in positions if p.get("symbol") and not p.get("error") and len(p.get("symbol", "")) > 10]
    except Exception:
        option_positions = []
    return {
        "status": "ok",
        "account_status": str(getattr(account, "status", "")),
        "paper": os.getenv("ALPACA_PAPER", "true").lower() == "true",
        "trading_enabled": os.getenv("ALPACA_TRADING_ENABLED", "false").lower() == "true",
        "trading_blocked": bool(getattr(account, "trading_blocked", False)),
        "account_blocked": bool(getattr(account, "account_blocked", False)),
        "cash": str(getattr(account, "cash", "")),
        "buying_power": str(getattr(account, "buying_power", "")),
        "equity": str(getattr(account, "equity", "")),
        "options_approved_level": getattr(account, "options_approved_level", None),
        "options_trading_level": getattr(account, "options_trading_level", None),
        "open_option_positions": len(option_positions),
        "positions": positions,
    }


def get_positions():
    """Fetch all open positions from Alpaca with P&L and DTE."""
    try:
        client = _alpaca_client()
        raw = client.get_all_positions()
        positions = []
        for pos in raw:
            symbol = str(getattr(pos, "symbol", "") or "")
            underlying, dte = _parse_occ_dte(symbol)
            qty = float(getattr(pos, "qty", 0) or 0)
            avg_entry = float(getattr(pos, "avg_entry_price", 0) or 0)
            current = float(getattr(pos, "current_price", 0) or 0)
            market_value = float(getattr(pos, "market_value", 0) or 0)
            unrealized_pl = float(getattr(pos, "unrealized_pl", 0) or 0)
            unrealized_plpc = float(getattr(pos, "unrealized_plpc", 0) or 0) * 100
            positions.append({
                "symbol": symbol,
                "underlying": underlying or symbol[:4],
                "qty": int(abs(qty)),
                "side": "long" if qty > 0 else "short",
                "avg_entry_price": round(avg_entry, 4),
                "current_price": round(current, 4),
                "market_value": round(market_value, 2),
                "unrealized_pl": round(unrealized_pl, 2),
                "unrealized_plpc": round(unrealized_plpc, 2),
                "dte": dte,
                "opened_at": datetime.now(timezone.utc).isoformat(),
            })
        return positions
    except Exception as exc:
        return [{"error": str(exc)[:200]}]


def close_position(symbol: str, qty: int = None, dry_run: bool = None):
    """Close an option position via market sell order."""
    trading_enabled = os.getenv("ALPACA_TRADING_ENABLED", "false").lower() == "true"
    should_dry_run = not trading_enabled if dry_run is None else dry_run

    if should_dry_run:
        return {
            "status": "dry_run",
            "symbol": symbol,
            "qty": qty,
            "side": "sell",
            "message": "Close order skipped. Set ALPACA_TRADING_ENABLED=true to submit.",
        }

    try:
        client = _alpaca_client()
        if qty is None:
            client.close_position(symbol)
            return {"status": "submitted", "symbol": symbol, "message": "Position close submitted"}
        market_order_data = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
        )
        order = client.submit_order(order_data=market_order_data)
        return {"status": "submitted", "order": str(order), "symbol": symbol, "qty": qty}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def execute_options_trade(symbol: str, qty: int, side: str, dry_run: bool = None):
    """
    Executes an options trade only when explicitly enabled.
    The symbol must be the specific OCC option symbol from the final options gate.
    """
    trading_enabled = os.getenv("ALPACA_TRADING_ENABLED", "false").lower() == "true"
    should_dry_run = not trading_enabled if dry_run is None else dry_run

    if should_dry_run:
        return {
            "status": "dry_run",
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "message": "Alpaca submission skipped. Set ALPACA_TRADING_ENABLED=true to submit real/paper orders."
        }

    try:
        order_side = OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL

        market_order_data = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=order_side,
            time_in_force=TimeInForce.DAY
        )

        market_order = _alpaca_client().submit_order(order_data=market_order_data)
        print(f"Order submitted: {market_order}")
        return {"status": "submitted", "order": market_order}
    except Exception as e:
        print(f"Failed to execute option trade: {e}")
        return {"status": "error", "message": str(e)}
