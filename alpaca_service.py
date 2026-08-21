import os
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


def alpaca_account_summary():
    account = _alpaca_client().get_account()
    return {
        "status": "ok",
        "account_status": str(getattr(account, "status", "")),
        "paper": os.getenv("ALPACA_PAPER", "true").lower() == "true",
        "trading_enabled": os.getenv("ALPACA_TRADING_ENABLED", "false").lower() == "true",
        "trading_blocked": bool(getattr(account, "trading_blocked", False)),
        "account_blocked": bool(getattr(account, "account_blocked", False)),
        "buying_power": str(getattr(account, "buying_power", "")),
        "options_approved_level": getattr(account, "options_approved_level", None),
        "options_trading_level": getattr(account, "options_trading_level", None),
    }


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
