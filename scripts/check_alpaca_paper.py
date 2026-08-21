#!/usr/bin/env python3
from pathlib import Path
import sys

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from alpaca_service import _alpaca_client


def main() -> int:
    load_dotenv(".env")
    account = _alpaca_client().get_account()
    print("alpaca_auth: ok")
    print("account_status:", getattr(account, "status", None))
    print("trading_blocked:", getattr(account, "trading_blocked", None))
    print("account_blocked:", getattr(account, "account_blocked", None))
    print("buying_power:", getattr(account, "buying_power", None))
    print("options_approved_level:", getattr(account, "options_approved_level", None))
    print("options_trading_level:", getattr(account, "options_trading_level", None))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
