from AlgorithmImports import *

import json
import os
from datetime import timedelta


class ResearchOptionsBot(QCAlgorithm):
    def initialize(self):
        self.set_start_date(2024, 1, 1)
        self.set_cash(100000)

        self.execute_orders = str(self.get_parameter("execute_orders") or "false").lower() == "true"
        self.max_positions = int(self.get_parameter("max_positions") or 2)
        self.research_file = self.get_parameter("research_file") or "runtime/lean_watchlist.json"
        self.candidates = self._load_candidates(self.research_file)

        self.underlyings = {}
        self.option_symbols = {}
        self.ema20 = {}
        self.ema50 = {}
        self.rsi_indicators = {}
        self.macd_indicators = {}
        self.last_signal_time = {}

        for candidate in self.candidates:
            ticker = candidate["ticker"]
            equity = self.add_equity(ticker, Resolution.MINUTE)
            option = self.add_option(ticker, Resolution.MINUTE)
            option.set_filter(lambda universe: universe.strikes(-8, 8).expiration(timedelta(days=25), timedelta(days=65)))

            self.underlyings[ticker] = equity.symbol
            self.option_symbols[ticker] = option.symbol
            self.ema20[ticker] = self.ema(ticker, 20, Resolution.DAILY)
            self.ema50[ticker] = self.ema(ticker, 50, Resolution.DAILY)
            self.rsi_indicators[ticker] = self.rsi(ticker, 14, MovingAverageType.WILDERS, Resolution.DAILY)
            self.macd_indicators[ticker] = self.macd(ticker, 12, 26, 9, MovingAverageType.EXPONENTIAL, Resolution.DAILY)

        self.set_warm_up(timedelta(days=70))
        self.debug(f"Loaded {len(self.candidates)} research candidates. execute_orders={self.execute_orders}")

    def on_data(self, data):
        if self.is_warming_up:
            return

        self._manage_existing_positions()

        if self._open_option_positions() >= self.max_positions:
            return

        for candidate in self.candidates:
            ticker = candidate["ticker"]
            if not self._technical_setup_ready(ticker, candidate["bias"]):
                continue
            if self._recently_signaled(ticker):
                continue

            chain = data.option_chains.get(self.option_symbols[ticker])
            if chain is None:
                continue

            contract = self._select_contract(chain, candidate)
            if contract is None:
                continue

            tag = (
                f"research={candidate.get('theme')} score={candidate.get('score')} "
                f"bias={candidate.get('bias')} planned={candidate.get('option_contract', {}).get('symbol')}"
            )
            self.last_signal_time[ticker] = self.time

            if self.execute_orders:
                self.market_order(contract.symbol, 1, tag=tag)
            else:
                self.debug(f"DRY RUN {ticker}: would buy {contract.symbol} {tag}")

    def _load_candidates(self, file_path):
        candidates = []
        search_paths = [
            file_path,
            os.path.join(os.getcwd(), file_path),
            os.path.join(os.getcwd(), "..", file_path),
            "/workspace/runtime/lean_watchlist.json",
            "/LeanCLI/runtime/lean_watchlist.json",
        ]
        for path in search_paths:
            if path and os.path.exists(path):
                with open(path, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                for item in payload.get("candidates", []):
                    if item.get("ticker") and item.get("option_contract"):
                        candidates.append(item)
                return candidates
        self.debug(f"Research file not found. Tried: {search_paths}")
        return candidates

    def _technical_setup_ready(self, ticker, bias):
        if not (
            self.ema20[ticker].is_ready
            and self.ema50[ticker].is_ready
            and self.rsi_indicators[ticker].is_ready
            and self.macd_indicators[ticker].is_ready
        ):
            return False

        price = self.securities[self.underlyings[ticker]].price
        ema20 = self.ema20[ticker].current.value
        ema50 = self.ema50[ticker].current.value
        rsi = self.rsi_indicators[ticker].current.value
        macd = self.macd_indicators[ticker]
        macd_bullish = macd.current.value > macd.signal.current.value

        if bias == "call":
            return price > ema20 > ema50 and 50 <= rsi <= 72 and macd_bullish
        return price < ema20 < ema50 and 28 <= rsi <= 50 and not macd_bullish

    def _select_contract(self, chain, candidate):
        planned = candidate.get("option_contract") or {}
        target_strike = float(planned.get("strike") or 0)
        target_expiration = planned.get("expiration")
        right = OptionRight.CALL if candidate.get("bias") == "call" else OptionRight.PUT

        contracts = [
            contract
            for contract in chain
            if contract.right == right
            and 25 <= (contract.expiry.date() - self.time.date()).days <= 65
            and contract.ask_price > 0
            and contract.bid_price > 0
        ]
        if not contracts:
            return None

        def score(contract):
            dte = (contract.expiry.date() - self.time.date()).days
            spread = (contract.ask_price - contract.bid_price) / max((contract.ask_price + contract.bid_price) / 2, 0.01)
            strike_distance = abs(float(contract.strike) - target_strike) if target_strike else 0
            expiry_penalty = 0 if str(contract.expiry.date()) == str(target_expiration) else 3
            return strike_distance + abs(dte - int(planned.get("dte") or 45)) * 0.1 + spread * 10 + expiry_penalty

        return sorted(contracts, key=score)[0]

    def _manage_existing_positions(self):
        for holding in self.portfolio.values():
            if not holding.invested or holding.symbol.security_type != SecurityType.OPTION:
                continue
            option_symbol = holding.symbol
            expiry = option_symbol.id.date
            if hasattr(expiry, "date"):
                expiry = expiry.date()
            dte = (expiry - self.time.date()).days
            if dte <= 14:
                if self.execute_orders:
                    self.liquidate(option_symbol, "Exit option before final two weeks")
                else:
                    self.debug(f"DRY RUN exit {option_symbol}: {dte} DTE")

    def _open_option_positions(self):
        return sum(
            1
            for holding in self.portfolio.values()
            if holding.invested and holding.symbol.security_type == SecurityType.OPTION
        )

    def _recently_signaled(self, ticker):
        previous = self.last_signal_time.get(ticker)
        return previous is not None and self.time - previous < timedelta(hours=6)
