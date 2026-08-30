from datetime import datetime, timezone
import os
import threading
from typing import Dict, Iterable, List, Optional, Set

from dotenv import load_dotenv

try:
    from alpaca.data.enums import OptionsFeed
    from alpaca.data.live import OptionDataStream
except Exception:
    OptionsFeed = None
    OptionDataStream = None


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


class AlpacaOptionStream:
    def __init__(self):
        self._lock = threading.RLock()
        self.stream = None
        self.thread: Optional[threading.Thread] = None
        self.status = "stopped"
        self.feed = os.getenv("ALPACA_OPTION_STREAM_FEED") or os.getenv("ALPACA_OPTION_FEED", "indicative")
        self.started_at = None
        self.last_error = None
        self.last_status_change = utc_now()
        # Ordered by the latest Trader/watchlist score (best contract first).
        self.target_symbols: List[str] = []
        self.subscribed_symbols: Set[str] = set()
        self.quotes: Dict[str, Dict] = {}
        self.sequence = 0
        self.client_count = 0

    def start(self) -> Dict:
        load_dotenv(override=True)
        with self._lock:
            self.feed = os.getenv("ALPACA_OPTION_STREAM_FEED") or os.getenv("ALPACA_OPTION_FEED", "indicative")
            if self.status in {"starting", "running"} and self.thread and self.thread.is_alive():
                return self.snapshot()

            api_key = os.getenv("ALPACA_API_KEY", "")
            secret_key = os.getenv("ALPACA_SECRET_KEY", "")
            if not api_key or not secret_key:
                self.status = "not_configured"
                self.last_error = "ALPACA_API_KEY/ALPACA_SECRET_KEY missing"
                self.last_status_change = utc_now()
                return self.snapshot()

            if OptionDataStream is None or OptionsFeed is None:
                self.status = "unavailable"
                self.last_error = "alpaca-py OptionDataStream unavailable"
                self.last_status_change = utc_now()
                return self.snapshot()

            try:
                self.stream = OptionDataStream(api_key, secret_key, feed=self._options_feed())
                self.status = "starting"
                self.started_at = utc_now()
                self.last_error = None
                self.last_status_change = utc_now()
                self.thread = threading.Thread(target=self._run_stream, name="alpaca-option-stream", daemon=True)
                self.thread.start()
            except Exception as exc:
                self.status = "error"
                self.last_error = str(exc)[:220]
                self.last_status_change = utc_now()
            return self.snapshot()

    def stop(self) -> Dict:
        with self._lock:
            stream = self.stream
            self.status = "stopped"
            self.last_status_change = utc_now()
        if stream:
            try:
                stream.stop()
            except Exception as exc:
                with self._lock:
                    self.last_error = str(exc)[:220]
        return self.snapshot()

    def set_symbols(self, symbols: Iterable[str]) -> Dict:
        clean_symbols = list(dict.fromkeys(symbol for symbol in symbols if symbol))
        clean_set = set(clean_symbols)
        self.start()
        with self._lock:
            targets_changed = clean_symbols != self.target_symbols
            self.target_symbols = clean_symbols
            stale_quotes = set(self.quotes) - clean_set
            if stale_quotes:
                self.quotes = {
                    symbol: self.quotes[symbol]
                    for symbol in clean_symbols
                    if symbol in self.quotes
                }
            if targets_changed or stale_quotes:
                self.sequence += 1
            stream = self.stream
            current = set(self.subscribed_symbols)
            to_unsubscribe = current - clean_set
            to_subscribe = [symbol for symbol in clean_symbols if symbol not in current]

        if stream:
            if to_unsubscribe:
                try:
                    stream.unsubscribe_quotes(*sorted(to_unsubscribe))
                except Exception as exc:
                    with self._lock:
                        self.last_error = str(exc)[:220]
                with self._lock:
                    self.subscribed_symbols -= to_unsubscribe
            if to_subscribe:
                try:
                    stream.subscribe_quotes(self._handle_quote, *to_subscribe)
                    with self._lock:
                        self.subscribed_symbols.update(to_subscribe)
                        self.status = "running"
                        self.last_status_change = utc_now()
                except Exception as exc:
                    with self._lock:
                        self.status = "error"
                        self.last_error = str(exc)[:220]
                        self.last_status_change = utc_now()

        return self.snapshot()

    def seed_quotes(self, quotes: Iterable[Dict]) -> Dict:
        with self._lock:
            for quote in quotes:
                symbol = quote.get("symbol")
                if not symbol:
                    continue
                bid = safe_float(quote.get("bid"))
                ask = safe_float(quote.get("ask"))
                mid = safe_float(quote.get("mid") or quote.get("price"))
                if mid is None and bid is not None and ask is not None:
                    mid = (bid + ask) / 2
                self.quotes[symbol] = {
                    "symbol": symbol,
                    "bid": safe_round(bid),
                    "ask": safe_round(ask),
                    "price": safe_round(mid),
                    "mid": safe_round(mid),
                    "feed": quote.get("feed") or self.feed,
                    "source": quote.get("provider") or "snapshot_seed",
                    "streamed": False,
                    "timestamp": quote.get("timestamp"),
                    "received_at": utc_now(),
                }
            self.sequence += 1
        return self.snapshot()

    def snapshot(self) -> Dict:
        with self._lock:
            # Only expose contracts from the latest scan, in Trader score order.
            quotes = {
                symbol: dict(self.quotes[symbol])
                for symbol in self.target_symbols
                if symbol in self.quotes
            }
            latest = sorted(
                quotes.values(),
                key=lambda item: item.get("received_at") or "",
                reverse=True,
            )
            stream_quote_count = sum(1 for quote in quotes.values() if quote.get("streamed"))
            subscribed_count = len(self.subscribed_symbols)
            status = self.status
            if status == "running" and subscribed_count and stream_quote_count == 0:
                detail = f"Subscribed to {subscribed_count} contracts; waiting for Alpaca stream ticks"
            elif status == "running":
                detail = f"Streaming {stream_quote_count} live quote symbols"
            elif status == "not_configured":
                detail = self.last_error or "Alpaca credentials missing"
            elif status == "unavailable":
                detail = self.last_error or "Alpaca stream SDK unavailable"
            elif status == "error":
                detail = self.last_error or "Option stream error"
            else:
                detail = "Option stream not started"

            return {
                "status": status,
                "detail": detail,
                "feed": self.feed,
                "source": "alpaca_option_stream",
                "started_at": self.started_at,
                "last_status_change": self.last_status_change,
                "last_error": self.last_error,
                "target_symbols": list(self.target_symbols),
                "subscribed_symbols": [
                    symbol for symbol in self.target_symbols
                    if symbol in self.subscribed_symbols
                ],
                "subscribed_count": subscribed_count,
                "quote_count": len(quotes),
                "stream_quote_count": stream_quote_count,
                "last_quote_at": latest[0].get("received_at") if latest else None,
                "quotes": quotes,
                "sequence": self.sequence,
                "client_count": self.client_count,
                "note": "Alpaca Basic uses the indicative options feed unless OPRA is configured.",
            }

    def client_connected(self) -> None:
        with self._lock:
            self.client_count += 1

    def client_disconnected(self) -> None:
        with self._lock:
            self.client_count = max(0, self.client_count - 1)

    def _run_stream(self) -> None:
        with self._lock:
            self.status = "running"
            self.last_status_change = utc_now()
        try:
            if self.stream:
                self.stream.run()
        except Exception as exc:
            with self._lock:
                self.status = "error"
                self.last_error = str(exc)[:220]
                self.last_status_change = utc_now()

    async def _handle_quote(self, data) -> None:
        symbol = getattr(data, "symbol", None)
        if not symbol:
            return
        bid = safe_float(getattr(data, "bid_price", None))
        ask = safe_float(getattr(data, "ask_price", None))
        mid = (bid + ask) / 2 if bid is not None and ask is not None else None
        timestamp = getattr(data, "timestamp", None)
        with self._lock:
            if symbol not in self.target_symbols:
                return
            self.quotes[symbol] = {
                "symbol": symbol,
                "bid": safe_round(bid),
                "ask": safe_round(ask),
                "price": safe_round(mid),
                "mid": safe_round(mid),
                "feed": self.feed,
                "source": "alpaca_stream",
                "streamed": True,
                "timestamp": timestamp.isoformat() if hasattr(timestamp, "isoformat") else timestamp,
                "received_at": utc_now(),
            }
            self.sequence += 1

    def _options_feed(self):
        if self.feed.lower() == "opra":
            return OptionsFeed.OPRA
        return OptionsFeed.INDICATIVE


alpaca_option_stream = AlpacaOptionStream()
