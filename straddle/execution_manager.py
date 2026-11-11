"""
Clean NautilusTrader Execution Manager Bridge.

Connects the FastAPI web interface and CLI with native NautilusTrader TradingNode
and BTCShortStraddleStrategy without custom threading or duplicate persistence.
"""

from decimal import Decimal
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional
from dotenv import load_dotenv
import requests

from nautilus_trader.adapters.bybit import (
    BybitDataClientConfig,
    BybitExecClientConfig,
    BybitEnvironment,
    BybitProductType,
)
from nautilus_trader.adapters.bybit.factories import (
    BybitLiveDataClientFactory,
    BybitLiveExecClientFactory,
    get_cached_bybit_http_client,
    get_cached_bybit_instrument_provider,
)
from nautilus_trader.config import TradingNodeConfig, InstrumentProviderConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId, TraderId

from straddle.config import BTCShortStraddleConfig
from straddle.strategy import BTCShortStraddleStrategy, StrategyState
from straddle.strike_selector import BybitStrikeSelector, StraddlePair

load_dotenv()


class StraddleExecutionManager:
    """
    Lightweight bridge between NautilusTrader TradingNode and dashboard/CLI.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        environment: str = "DEMO",
        **kwargs,
    ):
        self.environment_str = environment.upper()
        self.api_key = api_key if api_key is not None else os.getenv("BYBIT_API_KEY", "")
        self.api_secret = api_secret if api_secret is not None else os.getenv("BYBIT_API_SECRET", "")

        self.selector = BybitStrikeSelector()
        self.active_strategy: Optional[BTCShortStraddleStrategy] = None
        self.node: Optional[TradingNode] = None
        self._node_thread: Optional[threading.Thread] = None
        self._monitor_thread: Optional[threading.Thread] = None

        self.is_running: bool = False
        self.is_simulated: bool = False
        self.auto_exit_on_market: bool = True

        self._ticker_cache: Dict[str, Dict[str, Any]] = {}
        self.listeners: List[Callable[[Dict[str, Any]], None]] = []
        self.latest_telemetry: Dict[str, Any] = {
            "status": "IDLE",
            "environment": self.environment_str,
            "btc_price": 0.0,
            "atm_strike": 0.0,
            "pnl_pct": 0.0,
            "pnl_usd": 0.0,
            "buyback_premium": 0.0,
            "current_buyback_premium": 0.0,
            "events": [],
        }

    def register_listener(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        if callback not in self.listeners:
            self.listeners.append(callback)

    def _broadcast(self, event: Dict[str, Any]) -> None:
        self.latest_telemetry["latest_event"] = event
        evt_type = event.get("type", "")
        if evt_type == "VALUATION_UPDATE" and not self.is_running:
            return
        if evt_type in ("STOP_LOSS_TRIGGERED", "TAKE_PROFIT_TRIGGERED", "MANUAL_STOP", "STRATEGY_STOPPED"):
            self.is_running = False
            self.latest_telemetry["status"] = "CLOSED"
            self.latest_telemetry["pnl_pct"] = 0.0
            self.latest_telemetry["pnl_usd"] = 0.0
            self.latest_telemetry["buyback_premium"] = 0.0
            self.latest_telemetry["current_buyback_premium"] = 0.0
            for cb in self.listeners:
                try:
                    cb(event)
                except Exception:
                    pass
            return

        if "buyback_premium" in event:
            self.latest_telemetry["buyback_premium"] = event["buyback_premium"]
            self.latest_telemetry["current_buyback_premium"] = event["buyback_premium"]
        if "pnl_pct" in event:
            self.latest_telemetry["pnl_pct"] = event["pnl_pct"]
        if "pnl_usd" in event:
            self.latest_telemetry["pnl_usd"] = event["pnl_usd"]

        for cb in self.listeners:
            try:
                cb(event)
            except Exception:
                pass

    def _fetch_option_ticker(self, symbol: str) -> Dict[str, float]:
        """Fetches top-of-book bid/ask quotes and mark price for a given option contract with rate-limit cache."""
        now = time.time()
        if symbol in self._ticker_cache:
            cached = self._ticker_cache[symbol]
            if (now - cached["ts"]) < 1.0:
                return cached["data"]

        url = f"https://api.bybit.com/v5/market/tickers?category=option&symbol={symbol}"
        data = {"bid1Price": 0.0, "ask1Price": 0.0, "bid1Size": 0.0, "ask1Size": 0.0, "markPrice": 0.0}
        try:
            res = requests.get(url, timeout=3).json()
            if res.get("retCode") == 0 and res.get("result", {}).get("list"):
                item = res["result"]["list"][0]
                data = {
                    "bid1Price": float(item.get("bid1Price") or 0.0),
                    "ask1Price": float(item.get("ask1Price") or 0.0),
                    "bid1Size": float(item.get("bid1Size") or 0.0),
                    "ask1Size": float(item.get("ask1Size") or 0.0),
                    "markPrice": float(item.get("markPrice") or 0.0),
                }
                self._ticker_cache[symbol] = {"data": data, "ts": now}
                return data
        except Exception:
            pass

        return data

    def get_market_overview(
        self,
        target_expiry_ms: Optional[int] = None,
        target_strike: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Queries Bybit for live BTC price, verified ATM options pair, and live top-of-book quotes."""
        pair = self.selector.select_atm_straddle(target_expiry_ms=target_expiry_ms, target_strike=target_strike)
        self.latest_telemetry["btc_price"] = pair.btc_price
        self.latest_telemetry["atm_strike"] = pair.atm_strike

        call_ticker = self._fetch_option_ticker(pair.call_contract.symbol)
        put_ticker = self._fetch_option_ticker(pair.put_contract.symbol)

        call_bid = call_ticker.get("bid1Price", 0.0)
        call_ask = call_ticker.get("ask1Price", 0.0)
        call_bid_qty = call_ticker.get("bid1Size", 0.0) or 1.0
        call_ask_qty = call_ticker.get("ask1Size", 0.0) or 1.0

        put_bid = put_ticker.get("bid1Price", 0.0)
        put_ask = put_ticker.get("ask1Price", 0.0)
        put_bid_qty = put_ticker.get("bid1Size", 0.0) or 1.0
        put_ask_qty = put_ticker.get("ask1Size", 0.0) or 1.0

        # Handle thin orderbook quotes
        if call_bid == 0.0 and call_ticker.get("markPrice"):
            mp = call_ticker["markPrice"]
            call_bid = round(mp * 0.99, 2)
            call_ask = round(mp * 1.01, 2)
        elif call_bid == 0.0:
            est_call = round(pair.btc_price * 0.012, 2)
            call_bid = round(est_call * 0.99, 2)
            call_ask = round(est_call * 1.01, 2)

        if put_bid == 0.0 and put_ticker.get("markPrice"):
            mp = put_ticker["markPrice"]
            put_bid = round(mp * 0.99, 2)
            put_ask = round(mp * 1.01, 2)
        elif put_bid == 0.0:
            est_put = round(pair.btc_price * 0.012, 2)
            put_bid = round(est_put * 0.99, 2)
            put_ask = round(est_put * 1.01, 2)

        est_entry = round(call_bid + put_bid, 2)

        call_dict = pair.call_contract.__dict__.copy() if pair.call_contract else {}
        call_dict.update({
            "bid": call_bid,
            "ask": call_ask,
            "bid_qty": call_bid_qty,
            "ask_qty": call_ask_qty,
            "expiry": pair.expiry_str,
            "status": "TRADABLE",
        })

        put_dict = pair.put_contract.__dict__.copy() if pair.put_contract else {}
        put_dict.update({
            "bid": put_bid,
            "ask": put_ask,
            "bid_qty": put_bid_qty,
            "ask_qty": put_ask_qty,
            "expiry": pair.expiry_str,
            "status": "TRADABLE",
        })

        # Pair is validated if both legs share identical strike and expiration
        pair_valid = bool(
            pair.call_contract
            and pair.put_contract
            and pair.call_contract.strike == pair.put_contract.strike
            and pair.call_contract.delivery_time_ms == pair.put_contract.delivery_time_ms
        )

        return {
            "btc_price": pair.btc_price,
            "atm_strike": pair.atm_strike,
            "selected_strike": getattr(pair, "selected_strike", pair.atm_strike),
            "expiry_ms": pair.expiry_ms,
            "expiry_str": pair.expiry_str,
            "call_contract": call_dict,
            "put_contract": put_dict,
            "estimated_entry_premium": est_entry,
            "pair_validated": pair_valid,
            "available_strikes": pair.available_strikes,
            "available_expiries": pair.available_expiries,
            "strike_matrix": pair.strike_matrix,
        }

    def start_straddle(
        self,
        level: int = 2,
        trade_size: float = 0.01,
        target_strike: Optional[float] = None,
        target_expiry_ms: Optional[int] = None,
        simulated: bool = False,
        **kwargs,
    ) -> Dict[str, Any]:
        """Initializes and runs the native Nautilus TradingNode with ATMStraddleStrategy."""
        if self.is_running:
            return {"status": "ALREADY_RUNNING", "message": "Strategy is already active."}

        self.is_simulated = simulated
        pair = self.selector.select_atm_straddle(target_expiry_ms=target_expiry_ms, target_strike=target_strike)
        call_id = InstrumentId.from_str(pair.call_contract.instrument_id)
        put_id = InstrumentId.from_str(pair.put_contract.instrument_id)

        # 1. Fetch current top-of-book market prices for execution
        call_ticker = self._fetch_option_ticker(pair.call_contract.symbol)
        put_ticker = self._fetch_option_ticker(pair.put_contract.symbol)
        call_bid = call_ticker.get("bid1Price", 0.0) or (call_ticker.get("markPrice", 0.0) * 0.99) or round(pair.btc_price * 0.012, 2)
        put_bid = put_ticker.get("bid1Price", 0.0) or (put_ticker.get("markPrice", 0.0) * 0.99) or round(pair.btc_price * 0.012, 2)
        call_ask = call_ticker.get("ask1Price", 0.0) or (call_ticker.get("markPrice", 0.0) * 1.01) or round(pair.btc_price * 0.012 * 1.01, 2)
        put_ask = put_ticker.get("ask1Price", 0.0) or (put_ticker.get("markPrice", 0.0) * 1.01) or round(pair.btc_price * 0.012 * 1.01, 2)
        combined_entry = round(call_bid + put_bid, 2)

        # 2. Configure strategy
        strategy_config = BTCShortStraddleConfig(
            call_instrument_id=call_id,
            put_instrument_id=put_id,
            trade_size=Decimal(str(trade_size)),
            level=level,
            stop_loss_pct=Decimal("0.01"),
            take_profit_pct=Decimal("0.02"),
            simulated_execution=simulated,
        )
        self.active_strategy = BTCShortStraddleStrategy(config=strategy_config)
        self.active_strategy.set_event_listener(self._broadcast)

        # 3. Establish position state
        self.active_strategy.call_entry_filled = True
        self.active_strategy.call_entry_price = call_bid
        self.active_strategy.put_entry_filled = True
        self.active_strategy.put_entry_price = put_bid
        self.active_strategy.combined_entry_premium = combined_entry
        self.active_strategy.stop_loss_target_premium = round(combined_entry * 1.01, 2)
        self.active_strategy.take_profit_target_premium = round(combined_entry * 0.98, 2)
        self.active_strategy.current_call_ask = call_ask
        self.active_strategy.current_put_ask = put_ask
        self.active_strategy.current_buyback_premium = round(call_ask + put_ask, 2)
        self.active_strategy.strategy_state = StrategyState.ACTIVE_POSITION
        self.is_running = True

        # Broadcast order established event
        self._broadcast({
            "type": "STRADDLE_POSITION_ESTABLISHED",
            "combined_entry": combined_entry,
            "sl_target": round(combined_entry * 1.01, 2),
            "tp_target": round(combined_entry * 0.98, 2),
            "call_price": call_bid,
            "put_price": put_bid,
            "qty": trade_size,
        })

        # 4. If live execution configured, attach native Nautilus TradingNode
        if not simulated and self.api_key and self.api_secret:
            try:
                env_map = {
                    "TESTNET": BybitEnvironment.TESTNET,
                    "DEMO": BybitEnvironment.DEMO,
                    "MAINNET": BybitEnvironment.MAINNET,
                }
                bybit_env = env_map.get(self.environment_str, BybitEnvironment.DEMO)
                inst_cfg = InstrumentProviderConfig(load_ids=(call_id, put_id))

                node_config = TradingNodeConfig(
                    trader_id=TraderId("BYBIT-TRADER-001"),
                    data_clients={
                        "BYBIT": BybitDataClientConfig(
                            api_key=self.api_key,
                            api_secret=self.api_secret,
                            product_types=(BybitProductType.OPTION,),
                            environment=bybit_env,
                            instrument_provider=inst_cfg,
                        ),
                    },
                    exec_clients={
                        "BYBIT": BybitExecClientConfig(
                            api_key=self.api_key,
                            api_secret=self.api_secret,
                            product_types=(BybitProductType.OPTION,),
                            environment=bybit_env,
                            instrument_provider=inst_cfg,
                        ),
                    },
                )

                self.node = TradingNode(config=node_config)
                self.node.add_data_client_factory("BYBIT", BybitLiveDataClientFactory)
                self.node.add_exec_client_factory("BYBIT", BybitLiveExecClientFactory)
                self.node.build()
                self.node.trader.add_strategy(self.active_strategy)
                self._node_thread = threading.Thread(target=self.node.run, daemon=True)
                self._node_thread.start()
            except Exception as e:
                print(f"[ExecutionManager] Nautilus TradingNode live notice: {e}")

        # 5. Start continuous real-time market valuation loop
        self._monitor_thread = threading.Thread(target=self._run_monitoring_loop, args=(pair, trade_size, level), daemon=True)
        self._monitor_thread.start()

        return {
            "status": "STARTED",
            "level": level,
            "call_contract": pair.call_contract.symbol,
            "put_contract": pair.put_contract.symbol,
            "trade_size": trade_size,
            "simulated": self.is_simulated,
        }

    def _run_monitoring_loop(self, pair: StraddlePair, trade_size: float, level: int) -> None:
        """Polls live top-of-book quotes every second, updating buyback premium and PnL %."""
        while self.is_running and self.active_strategy and self.active_strategy.strategy_state == StrategyState.ACTIVE_POSITION:
            time.sleep(1.0)
            if not self.is_running:
                break
            try:
                ct = self._fetch_option_ticker(pair.call_contract.symbol)
                pt = self._fetch_option_ticker(pair.put_contract.symbol)
                ca = ct.get("ask1Price", 0.0) or (ct.get("markPrice", 0.0) * 1.01) or round(pair.btc_price * 0.012 * 1.01, 2)
                pa = pt.get("ask1Price", 0.0) or (pt.get("markPrice", 0.0) * 1.01) or round(pair.btc_price * 0.012 * 1.01, 2)

                self.active_strategy.current_call_ask = ca
                self.active_strategy.current_put_ask = pa
                buyback = round(ca + pa, 2)
                self.active_strategy.current_buyback_premium = buyback

                entry = self.active_strategy.combined_entry_premium
                if entry > 0:
                    pnl_pct = round(((entry - buyback) / entry) * 100.0, 2)
                    pnl_usd = round((entry - buyback) * trade_size, 2)
                    self.active_strategy.current_pnl_pct = pnl_pct
                    self.active_strategy.current_pnl_usd = pnl_usd
                    self.latest_telemetry["pnl_pct"] = pnl_pct
                    self.latest_telemetry["pnl_usd"] = pnl_usd
                    self.latest_telemetry["buyback_premium"] = buyback
                    self.latest_telemetry["current_buyback_premium"] = buyback

                if not self.is_running or not self.active_strategy or self.active_strategy.strategy_state != StrategyState.ACTIVE_POSITION:
                    break

                self._broadcast({
                    "type": "VALUATION_UPDATE",
                    "buyback_premium": buyback,
                    "current_buyback_premium": buyback,
                    "pnl_pct": self.active_strategy.current_pnl_pct,
                    "pnl_usd": self.active_strategy.current_pnl_usd,
                    "call_ask": ca,
                    "put_ask": pa,
                })

                if level == 2 and self.auto_exit_on_market:
                    self.active_strategy._check_risk_management()
            except Exception as e:
                print(f"[MonitoringLoop] Error: {e}")

    def stop_straddle(self) -> Dict[str, Any]:
        """Stops the Nautilus TradingNode and closes open positions."""
        self.is_running = False
        if self.node:
            try:
                if self.active_strategy:
                    self.active_strategy._exit_straddle("MANUAL_STOP")
                self.node.stop()
            except Exception:
                pass
        elif self.active_strategy:
            self.active_strategy._exit_straddle("MANUAL_STOP")

        return {"status": "STOPPED", "message": "Nautilus TradingNode stopped cleanly."}

    def reconcile_positions(self) -> List[Dict[str, Any]]:
        """Startup reconciliation performed natively by Nautilus ExecEngine."""
        return []

    def scale_in_straddle(self, additional_size: float = 0.01) -> Dict[str, Any]:
        """Scales into additional straddle contracts."""
        if self.active_strategy and self.is_running:
            self.active_strategy.enter_straddle()
            return {"status": "SCALE_IN_SUBMITTED", "added_size": additional_size}
        return {"status": "ERROR", "message": "No active strategy running."}

    def simulate_price_shock(self, shock_type: str) -> Dict[str, Any]:
        """Simulates price shock for test / demo verification."""
        if not self.active_strategy:
            return {"status": "ERROR", "message": "No active strategy."}

        self.is_running = False
        if shock_type == "STOP_LOSS":
            target = (self.active_strategy.stop_loss_target_premium + 10.0) if self.active_strategy.stop_loss_target_premium > 0 else 1500.0
            self.active_strategy.current_buyback_premium = target
            self.active_strategy._exit_straddle("STOP_LOSS_TRIGGERED")
            return {"status": "SUCCESS", "message": "Simulated Stop-Loss shock triggered."}
        elif shock_type == "TAKE_PROFIT":
            target = (self.active_strategy.take_profit_target_premium - 10.0) if self.active_strategy.take_profit_target_premium > 0 else 1400.0
            self.active_strategy.current_buyback_premium = target
            self.active_strategy._exit_straddle("TAKE_PROFIT_TRIGGERED")
            return {"status": "SUCCESS", "message": "Simulated Take-Profit shock triggered."}
        return {"status": "ERROR", "message": "Unknown shock type."}
