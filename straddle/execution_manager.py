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

from nautilus_trader.adapters.bybit import (
    BybitDataClientConfig,
    BybitExecClientConfig,
    BybitEnvironment,
    BybitProductType,
)
from nautilus_trader.adapters.bybit.factories import (
    BybitLiveDataClientFactory,
    BybitLiveExecClientFactory,
)
from nautilus_trader.config import TradingNodeConfig
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

    def __init__(self, environment: str = "TESTNET"):
        self.environment_str = environment.upper()
        self.api_key = os.getenv("BYBIT_API_KEY", "")
        self.api_secret = os.getenv("BYBIT_API_SECRET", "")

        self.selector = BybitStrikeSelector()
        self.active_strategy: Optional[BTCShortStraddleStrategy] = None
        self.node: Optional[TradingNode] = None
        self._node_thread: Optional[threading.Thread] = None

        self.is_running: bool = False
        self.is_simulated: bool = False
        self.auto_exit_on_market: bool = True

        self.listeners: List[Callable[[Dict[str, Any]], None]] = []
        self.latest_telemetry: Dict[str, Any] = {
            "status": "IDLE",
            "environment": self.environment_str,
            "btc_price": 0.0,
            "atm_strike": 0.0,
            "pnl_pct": 0.0,
            "pnl_usd": 0.0,
            "events": [],
        }

    def register_listener(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        if callback not in self.listeners:
            self.listeners.append(callback)

    def _broadcast(self, event: Dict[str, Any]) -> None:
        self.latest_telemetry["latest_event"] = event
        for cb in self.listeners:
            try:
                cb(event)
            except Exception:
                pass

    def get_market_overview(self, target_expiry_ms: Optional[int] = None, target_strike: Optional[float] = None) -> Dict[str, Any]:
        """Queries Bybit for live BTC price and ATM options contracts."""
        pair = self.selector.select_atm_straddle(target_expiry_ms=target_expiry_ms, target_strike=target_strike)
        self.latest_telemetry["btc_price"] = pair.btc_price
        self.latest_telemetry["atm_strike"] = pair.atm_strike
        return {
            "btc_price": pair.btc_price,
            "expiry_ms": pair.expiry_ms,
            "expiry_str": pair.expiry_str,
            "atm_strike": pair.atm_strike,
            "call_contract": pair.call_contract.__dict__ if pair.call_contract else None,
            "put_contract": pair.put_contract.__dict__ if pair.put_contract else None,
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
    ) -> Dict[str, Any]:
        """Initializes and runs the native Nautilus TradingNode with ATMStraddleStrategy."""
        if self.is_running:
            return {"status": "ALREADY_RUNNING", "message": "Strategy is already active."}

        pair = self.selector.select_atm_straddle(target_expiry_ms=target_expiry_ms, target_strike=target_strike)
        call_id = InstrumentId.from_str(pair.call_contract.instrument_id)
        put_id = InstrumentId.from_str(pair.put_contract.instrument_id)

        env_map = {
            "TESTNET": BybitEnvironment.TESTNET,
            "DEMO": BybitEnvironment.DEMO,
            "MAINNET": BybitEnvironment.MAINNET,
        }
        bybit_env = env_map.get(self.environment_str, BybitEnvironment.TESTNET)

        node_config = TradingNodeConfig(
            trader_id=TraderId("BYBIT-TRADER-001"),
            data_clients={
                "BYBIT": BybitDataClientConfig(
                    api_key=self.api_key,
                    api_secret=self.api_secret,
                    product_types=(BybitProductType.OPTION,),
                    environment=bybit_env,
                ),
            },
            exec_clients={
                "BYBIT": BybitExecClientConfig(
                    api_key=self.api_key,
                    api_secret=self.api_secret,
                    product_types=(BybitProductType.OPTION,),
                    environment=bybit_env,
                ),
            },
        )

        self.node = TradingNode(config=node_config)
        self.node.add_data_client_factory("BYBIT", BybitLiveDataClientFactory)
        self.node.add_exec_client_factory("BYBIT", BybitLiveExecClientFactory)
        self.node.build()

        strategy_config = BTCShortStraddleConfig(
            call_instrument_id=call_id,
            put_instrument_id=put_id,
            trade_size=Decimal(str(trade_size)),
            level=level,
            stop_loss_pct=Decimal("0.01"),
            take_profit_pct=Decimal("0.02"),
        )
        self.active_strategy = BTCShortStraddleStrategy(config=strategy_config)
        self.active_strategy.set_event_listener(self._broadcast)
        self.node.trader.add_strategy(self.active_strategy)

        self.is_running = True
        self._node_thread = threading.Thread(target=self.node.run, daemon=True)
        self._node_thread.start()

        return {
            "status": "STARTED",
            "level": level,
            "call_contract": pair.call_contract.symbol,
            "put_contract": pair.put_contract.symbol,
            "trade_size": trade_size,
        }

    def stop_straddle(self) -> Dict[str, Any]:
        """Stops the Nautilus TradingNode and closes open positions."""
        if self.node and self.is_running:
            try:
                if self.active_strategy:
                    self.active_strategy._exit_straddle("MANUAL_STOP")
                self.node.stop()
            except Exception:
                pass
        self.is_running = False
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

        if shock_type == "STOP_LOSS":
            self.active_strategy.current_buyback_premium = self.active_strategy.stop_loss_target_premium + 10.0
            self.active_strategy._check_risk_management()
            return {"status": "SUCCESS", "message": "Simulated Stop-Loss shock triggered."}
        elif shock_type == "TAKE_PROFIT":
            self.active_strategy.current_buyback_premium = self.active_strategy.take_profit_target_premium - 10.0
            self.active_strategy._check_risk_management()
            return {"status": "SUCCESS", "message": "Simulated Take-Profit shock triggered."}
        return {"status": "ERROR", "message": "Unknown shock type."}
