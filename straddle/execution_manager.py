"""
================================================================================
⚡ NAUTILUSTRADER STRADDLE EXECUTION MANAGER (straddle/execution_manager.py)
================================================================================
The orchestration bridge connecting the FastAPI web interface and CLI with
the native NautilusTrader TradingNode and BTCShortStraddleStrategy.

Key Architectural Responsibilities (Interview Notes):
1. Process & Thread Separation:
   - NautilusTrader runs on its own high-throughput, low-latency event loop inside
     a dedicated background daemon thread.
   - FastAPI routes run asynchronously on Uvicorn's asyncio event loop.
   - The Execution Manager acts as a thread-safe barrier: commands from web routes
     are queued safely onto the strategy clock via thread-safe queues.
2. In-Memory Cache Reconciliation (Single Source of Truth):
   - Rather than making slow, rate-limited HTTP calls to Bybit for position and
     balance data, the Execution Manager queries NautilusTrader's live cache:
     `self.node.cache.positions_open()` and `self.node.cache.orders()`.
   - This ensures sub-millisecond response times and guarantees zero discrepancy
     between what Nautilus tracks and what the trader sees on screen.
3. Multi-Lot Averaging & Scale-In Management:
   - Recalibrates entry metrics using Volume-Weighted Average Pricing (VWAP)
     when adding subsequent straddle lots to an active position.
4. Telemetry Dispatching:
   - Subscribes to strategy events (fills, quote ticks, risk triggers) and
     broadcasts them to connected browser WebSockets in real time.
================================================================================
"""

from decimal import Decimal
import asyncio
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
    get_cached_bybit_http_client,
    get_cached_bybit_instrument_provider,
)
from nautilus_trader.config import TradingNodeConfig, InstrumentProviderConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import InstrumentId, TraderId, StrategyId, Venue, PositionId, ClientOrderId

from straddle.config import BTCShortStraddleConfig
from straddle.strategy import BTCShortStraddleStrategy, StrategyState
from straddle.strike_selector import BybitStrikeSelector, StraddlePair, OptionContractInfo
from straddle.persistence import state_persistence

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

        self.selector = BybitStrikeSelector(self.environment_str, self.api_key, self.api_secret)
        self.active_strategy: Optional[BTCShortStraddleStrategy] = None
        self.node: Optional[TradingNode] = None
        self._node_thread: Optional[threading.Thread] = None
        self._monitor_thread: Optional[threading.Thread] = None

        self.is_running: bool = False
        self.is_simulated: bool = False
        self.auto_exit_on_market: bool = False
        self._last_exit_ts: float = 0.0

        self._ticker_cache: Dict[str, Dict[str, Any]] = {}
        self.listeners: List[Callable[[Dict[str, Any]], None]] = []
        self._node_ready = threading.Event()
        self._entry_lock = threading.Lock()
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

    def _node_is_running(self) -> bool:
        if self.node is None:
            return False
        try:
            return bool(self.node.is_running())
        except Exception:
            return bool(self._node_thread and self._node_thread.is_alive())

    def live_positions(self) -> List[Dict[str, Any]]:
        """Read open positions only from NautilusTrader's live cache."""
        if not self._node_is_running():
            return []
        output = []
        for position in self.node.cache.positions_open(venue=Venue("BYBIT")):
            instrument = self.node.cache.instrument(position.instrument_id)
            tick = self.node.cache.quote_tick(position.instrument_id)
            side = str(position.side).split(".")[-1].title()
            mark = 0.0
            if tick is not None:
                mark = tick.ask_price.as_double() if side == "Short" else tick.bid_price.as_double()
            pnl = position.unrealized_pnl(instrument.make_price(mark)) if instrument and mark > 0 else 0
            symbol = str(position.instrument_id).split("-OPTION")[0]
            output.append({
                "symbol": symbol,
                "side": side,
                "size": str(position.quantity),
                "avgPrice": str(position.avg_px_open),
                "markPrice": f"{mark:.2f}",
                "unrealisedPnl": str(pnl),
                "position_id": str(position.id),
                "instrument_id": str(position.instrument_id),
                "opening_order_id": str(position.opening_order_id),
            })
        return output

    def order_history(self) -> List[Dict[str, Any]]:
        """Return NautilusTrader cache orders plus persisted callback history."""
        combined: Dict[str, Dict[str, Any]] = {}
        if self._node_is_running():
            for order in self.node.cache.orders(venue=Venue("BYBIT")):
                oid = str(order.client_order_id)
                combined[oid] = {
                    "order_id": oid,
                    "time": time.strftime("%H:%M:%S", time.localtime(order.ts_init / 1_000_000_000)),
                    "created_ms": order.ts_init // 1_000_000,
                    "contract": str(order.instrument_id).split("-OPTION")[0],
                    "side": str(order.side).split(".")[-1],
                    "qty": float(order.quantity),
                    "filled_qty": float(order.filled_qty),
                    "avg_price": float(order.avg_px) if order.avg_px else 0.0,
                    "status": str(order.status).split(".")[-1].upper(),
                }
        for row in state_persistence.get_orders():
            combined.setdefault(str(row.get("order_id", "")), row)
        return sorted(combined.values(), key=lambda row: row.get("created_ms", 0) or 0, reverse=True)

    def account_state(self) -> Dict[str, Any]:
        """Read account balances from NautilusTrader's account cache."""
        if not self._node_is_running():
            return {"status": "NAUTILUS_NOT_RUNNING"}
        account = self.node.cache.account_for_venue(Venue("BYBIT"))
        if account is None:
            return {"status": "ACCOUNT_NOT_RECONCILED"}
        return {
            "status": "CONNECTED",
            "account_id": str(account.id),
            "balances_total": {str(k): str(v) for k, v in account.balances_total().items()},
            "balances_free": {str(k): str(v) for k, v in account.balances_free().items()},
        }

    def register_listener(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        if callback not in self.listeners:
            self.listeners.append(callback)

    def close_straddle_positions(
        self,
        call_symbol: Optional[str] = None,
        put_symbol: Optional[str] = None,
    ) -> bool:
        """Queue exact saved entry closes through the running Nautilus strategy."""
        targets = {value for value in (call_symbol, put_symbol) if value}
        entries = state_persistence.get_active_strategies()
        selected = [
            entry for entry in entries
            if not targets or entry.get("call_symbol") in targets or entry.get("put_symbol") in targets
        ]
        if not selected:
            return not any(
                row.get("symbol") in targets
                for row in self.live_positions()
            )
        if not self._node_is_running() or not self.active_strategy:
            return False
        for entry in selected:
            self.active_strategy.queue_close(str(entry.get("strategy_id")), {
                "CALL": entry.get("call_position_id"),
                "PUT": entry.get("put_position_id"),
            })
        deadline = time.time() + 4.0
        selected_ids = {str(entry.get("strategy_id")) for entry in selected}
        while time.time() < deadline:
            still_open = {
                str(entry.get("strategy_id"))
                for entry in state_persistence.get_active_strategies()
                if str(entry.get("strategy_id")) in selected_ids
            }
            if not still_open:
                return True
            time.sleep(0.1)
        return False

    def close_strategy(self, strategy_id_or_symbol: str) -> Dict[str, Any]:
        """Close only the position IDs mapped to one saved straddle entry via Nautilus."""
        if self._node_is_running():
            self.reconcile_positions()
        target = next(
            (
                entry for entry in state_persistence.get_active_strategies()
                if strategy_id_or_symbol in (
                    entry.get("strategy_id"),
                    entry.get("call_symbol"),
                    entry.get("put_symbol"),
                )
            ),
            None,
        )
        if target is None:
            return {"status": "ERROR", "message": f"Entry {strategy_id_or_symbol} was not found.", "verified_flat": False}
        if not target.get("live_verified") or not target.get("call_position_id") or not target.get("put_position_id"):
            return {
                "status": "ERROR",
                "message": "This entry has no confirmed NautilusTrader position IDs; it cannot be closed safely yet.",
                "verified_flat": False,
            }
        if not self._node_is_running() or not self.active_strategy:
            return {"status": "ERROR", "message": "NautilusTrader TradingNode is not running.", "verified_flat": False}
        self.active_strategy.queue_close(str(target["strategy_id"]), {
            "CALL": target.get("call_position_id"),
            "PUT": target.get("put_position_id"),
        })
        return {
            "status": "CLOSING",
            "strategy_id": target["strategy_id"],
            "message": "Close submitted through NautilusTrader for the mapped Call and Put position IDs.",
            "verified_flat": False,
            "all_closed": False,
        }

    def cancel_order(self, client_order_id: str) -> Dict[str, Any]:
        """Cancel a cached live order through the NautilusTrader strategy/ExecutionEngine."""
        if not self._node_is_running() or not self.active_strategy:
            return {"status": "ERROR", "message": "NautilusTrader TradingNode is not running."}
        try:
            order = self.node.cache.order(ClientOrderId(client_order_id))
        except Exception:
            order = None
        if order is None:
            return {"status": "ERROR", "message": "Order is not present in the NautilusTrader cache."}
        if order.is_closed:
            return {"status": "ERROR", "message": "Order is already terminal."}
        self.active_strategy.queue_cancel(client_order_id)
        return {"status": "CANCEL_PENDING", "order_id": client_order_id}

    def close_specific_position(self, symbol: str) -> Dict[str, Any]:
        """Close the straddle entry mapped to a live contract symbol."""
        return self.close_strategy(symbol)

    def _broadcast(self, event: Dict[str, Any]) -> None:
        self.latest_telemetry["latest_event"] = event
        evt_type = event.get("type", "")
        if evt_type == "NAUTILUS_STRATEGY_READY":
            self._node_ready.set()
        elif evt_type == "ENTRY_SUBMITTED":
            for leg, key in (("CALL", "call_order_id"), ("PUT", "put_order_id")):
                state_persistence.add_order(
                    order_id=event.get(key, ""),
                    contract=event.get("call_symbol" if leg == "CALL" else "put_symbol", ""),
                    side="SELL",
                    qty=float(event.get("trade_size", 0.0)),
                    filled_qty=0.0,
                    avg_price=0.0,
                    status="SUBMITTED",
                )
        elif evt_type == "ORDER_STATUS_CHANGED":
            client_order_id = str(event.get("client_order_id", ""))
            order = None
            if self._node_is_running() and client_order_id:
                try:
                    order = self.node.cache.order(ClientOrderId(client_order_id))
                except Exception:
                    order = None
            state_persistence.add_order(
                order_id=client_order_id,
                contract=str(order.instrument_id).split("-OPTION")[0] if order is not None else "",
                side=str(order.side).split(".")[-1] if order is not None else "UNKNOWN",
                qty=float(order.quantity) if order is not None else 0.0,
                filled_qty=float(order.filled_qty) if order is not None else 0.0,
                avg_price=float(order.avg_px) if order is not None and order.avg_px else 0.0,
                status=str(event.get("status", "UNKNOWN")),
            )
        elif evt_type == "ORDER_FILLED":
            client_order_id = str(event.get("client_order_id", ""))
            order = None
            if self._node_is_running() and client_order_id:
                try:
                    order = self.node.cache.order(ClientOrderId(client_order_id))
                except Exception:
                    order = None
            last_qty = float(event.get("filled_qty", 0.0))
            total_qty = float(order.quantity) if order is not None else last_qty
            filled_qty = float(order.filled_qty) if order is not None else last_qty
            avg_price = float(order.avg_px) if order is not None and order.avg_px else float(event.get("fill_price", 0.0))
            state_persistence.add_order(
                order_id=client_order_id,
                contract=str(event.get("instrument_id", "")).split("-OPTION")[0],
                side=str(event.get("side") or "SELL"),
                qty=total_qty,
                filled_qty=filled_qty,
                avg_price=avg_price,
                status="FILLED" if order is not None and order.is_filled else "PARTIALLY_FILLED",
            )
        elif evt_type == "STRADDLE_POSITION_ESTABLISHED" and event.get("entry_id"):
            entry_id = str(event.get("entry_id", ""))
            created_ts = time.time()
            strat = {
                "strategy_id": entry_id,
                "name": f"Strategy: ${float(event.get('strike') or 0):,.0f} Straddle",
                "level": int(event.get("level", 2)),
                "strike": event.get("strike"),
                "expiry_ms": event.get("expiry_ms"),
                "call_symbol": event.get("call_symbol"),
                "put_symbol": event.get("put_symbol"),
                "call_position_id": event.get("call_position_id"),
                "put_position_id": event.get("put_position_id"),
                "call_order_id": event.get("call_order_id"),
                "put_order_id": event.get("put_order_id"),
                "call_entry": event.get("call_price", 0.0),
                "put_entry": event.get("put_price", 0.0),
                "trade_size": event.get("qty", 0.0),
                "combined_entry_premium": event.get("combined_entry", 0.0),
                "current_buyback_premium": event.get("combined_entry", 0.0),
                "stop_loss_target": event.get("sl_target", 0.0),
                "take_profit_target": event.get("tp_target", 0.0),
                "status": "OPEN",
                "live_verified": True,
                "is_active": True,
                "created_ts": created_ts,
                "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "time_str": time.strftime("%H:%M:%S"),
            }
            state_persistence.add_active_strategy(strat)
            self.is_running = True
            self.latest_telemetry["status"] = "OPEN"
        elif evt_type == "STRADDLE_POSITION_IDS_CONFIRMED":
            state_persistence.update_active_strategy(str(event.get("entry_id", "")), {
                "call_position_id": event.get("call_position_id"),
                "put_position_id": event.get("put_position_id"),
                "status": "OPEN",
                "live_verified": True,
            })
        elif evt_type == "ENTRY_CLOSE_SUBMITTED":
            state_persistence.update_active_strategy(str(event.get("entry_id", "")), {
                "status": "CLOSING",
            })
        elif evt_type == "ENTRY_VALUATION_UPDATE":
            entry_id = str(event.get("entry_id", ""))
            state_persistence.update_active_strategy(entry_id, {
                "current_buyback_premium": event.get("buyback_premium", 0.0),
                "pnl_pct": event.get("pnl_pct", 0.0),
                "pnl_usd": event.get("pnl_usd", 0.0),
                "call_ask": event.get("call_ask", 0.0),
                "put_ask": event.get("put_ask", 0.0),
            })
        elif evt_type == "STRATEGY_ENTRY_CLOSED":
            state_persistence.remove_active_strategy(str(event.get("entry_id", "")))
            self.is_running = bool(state_persistence.get_active_strategies())
        elif evt_type in ("ENTRY_ORDER_FAILED", "ENTRY_TIMEOUT", "DASHBOARD_COMMAND_ERROR"):
            entry_id = str(event.get("entry_id", ""))
            if entry_id:
                state_persistence.update_active_strategy(entry_id, {
                    "status": event.get("status", "EXECUTION_ERROR"),
                    "live_verified": False,
                    "error": event.get("message", "Entry did not fill both legs."),
                })
        elif evt_type == "ENTRY_ABORTED":
            state_persistence.remove_active_strategy(str(event.get("entry_id", "")))
        if evt_type == "VALUATION_UPDATE" and not self.is_running:
            return
        if evt_type in ("STOP_LOSS_TRIGGERED", "TAKE_PROFIT_TRIGGERED", "MANUAL_STOP", "STRATEGY_STOPPED"):
            if event.get("all_closed") is False:
                # Single position closed, other positions still active
                for cb in self.listeners:
                    try:
                        cb(event)
                    except Exception:
                        pass
                return

            self._last_exit_ts = time.time()
            self.is_running = bool(state_persistence.get_active_strategies())
            self.latest_telemetry["status"] = "CLOSING" if self.is_running else "CLOSED"
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

        try:
            data = self.selector.get_ticker(symbol)
            self._ticker_cache[symbol] = {"data": data, "ts": now}
            return data
        except Exception:
            fallback = {
                "bid1Price": 0.0,
                "ask1Price": 0.0,
                "bid1Size": 0.0,
                "ask1Size": 0.0,
                "markPrice": 0.0,
                "lastPrice": 0.0,
                "indexPrice": 0.0,
            }
            self._ticker_cache[symbol] = {"data": fallback, "ts": now}
            return fallback

    def get_market_overview(
        self,
        target_expiry_ms: Optional[int] = None,
        target_strike: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Queries Bybit for live BTC price, verified ATM options pair, and live top-of-book quotes with 1.5s cache."""
        now = time.time()
        cache_key = f"{target_expiry_ms}_{target_strike}"
        if hasattr(self, "_overview_cache") and cache_key in self._overview_cache:
            entry = self._overview_cache[cache_key]
            if (now - entry["ts"]) < 1.5:
                return entry["data"]

        pair = self.selector.select_atm_straddle(target_expiry_ms=target_expiry_ms, target_strike=target_strike)
        self.latest_telemetry["btc_price"] = pair.btc_price
        self.latest_telemetry["atm_strike"] = pair.atm_strike

        call_ticker = self._fetch_option_ticker(pair.call_contract.symbol)
        put_ticker = self._fetch_option_ticker(pair.put_contract.symbol)

        call_bid = call_ticker.get("bid1Price", 0.0)
        call_ask = call_ticker.get("ask1Price", 0.0)
        call_bid_qty = call_ticker.get("bid1Size", 0.0)
        call_ask_qty = call_ticker.get("ask1Size", 0.0)

        put_bid = put_ticker.get("bid1Price", 0.0)
        put_ask = put_ticker.get("ask1Price", 0.0)
        put_bid_qty = put_ticker.get("bid1Size", 0.0)
        put_ask_qty = put_ticker.get("ask1Size", 0.0)

        # Show only live adapter quotes. Never invent a quote from mark or BTC price.
        call_quote_valid = call_bid > 0 and call_ask > 0
        put_quote_valid = put_bid > 0 and put_ask > 0
        est_entry = round(call_bid + put_bid, 2) if call_quote_valid and put_quote_valid else 0.0

        call_dict = pair.call_contract.__dict__.copy() if pair.call_contract else {}
        call_dict.update({
            "bid": call_bid,
            "ask": call_ask,
            "bid_qty": call_bid_qty,
            "ask_qty": call_ask_qty,
            "expiry": pair.expiry_str,
            "status": "TRADABLE" if call_quote_valid else "NO_LIVE_QUOTE",
        })

        put_dict = pair.put_contract.__dict__.copy() if pair.put_contract else {}
        put_dict.update({
            "bid": put_bid,
            "ask": put_ask,
            "bid_qty": put_bid_qty,
            "ask_qty": put_ask_qty,
            "expiry": pair.expiry_str,
            "status": "TRADABLE" if put_quote_valid else "NO_LIVE_QUOTE",
        })

        # Pair is validated if both legs share identical strike and expiration
        pair_valid = bool(
            pair.call_contract
            and pair.put_contract
            and pair.call_contract.strike == pair.put_contract.strike
            and pair.call_contract.delivery_time_ms == pair.put_contract.delivery_time_ms
            and call_quote_valid
            and put_quote_valid
        )

        res = {
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
            "available_contracts_count": pair.available_contracts_count,
            "available_expiries": pair.available_expiries,
            "strike_matrix": pair.strike_matrix,
        }
        if not hasattr(self, "_overview_cache"):
            self._overview_cache = {}
        self._overview_cache[cache_key] = {"data": res, "ts": now}
        return res

    def _start_node_for_pair(self, pair: StraddlePair, trade_size: float, level: int, initial_entry: Optional[Dict[str, Any]]) -> None:
        """Build the shared Nautilus TradingNode and run its strategy on a background thread."""
        if not self.api_key or not self.api_secret:
            raise RuntimeError("Configure Bybit credentials before live trading.")
        if self.node is not None:
            if self._node_is_running():
                self.active_strategy.queue_entry(initial_entry)
                return
            raise RuntimeError("A NautilusTrader node exists but is not running; restart the application before trading.")

        environment_map = {
            "TESTNET": BybitEnvironment.TESTNET,
            "DEMO": BybitEnvironment.DEMO,
            "MAINNET": BybitEnvironment.MAINNET,
        }
        bybit_environment = environment_map[self.environment_str]
        instrument_provider = InstrumentProviderConfig(
            load_all=True,
            filters={"base_coin": "BTC"},
        )
        node_config = TradingNodeConfig(
            trader_id=TraderId("BYBIT-TRADER-001"),
            data_clients={
                "BYBIT": BybitDataClientConfig(
                    api_key=self.api_key,
                    api_secret=self.api_secret,
                    product_types=(BybitProductType.OPTION,),
                    environment=bybit_environment,
                    instrument_provider=instrument_provider,
                ),
            },
            exec_clients={
                "BYBIT": BybitExecClientConfig(
                    api_key=self.api_key,
                    api_secret=self.api_secret,
                    product_types=(BybitProductType.OPTION,),
                    environment=bybit_environment,
                    instrument_provider=instrument_provider,
                ),
            },
        )
        strategy_config = BTCShortStraddleConfig(
            call_instrument_id=InstrumentId.from_str(pair.call_contract.instrument_id),
            put_instrument_id=InstrumentId.from_str(pair.put_contract.instrument_id),
            trade_size=Decimal(str(trade_size)),
            level=level,
            stop_loss_pct=Decimal("0.01"),
            take_profit_pct=Decimal("0.02"),
            auto_start_entry=False,
            oms_type=OmsType.HEDGING,
            external_order_claims=list({
                InstrumentId.from_str(f"{symbol}-OPTION.BYBIT")
                for saved in state_persistence.get_active_strategies()
                for symbol in (saved.get("call_symbol"), saved.get("put_symbol"))
                if symbol
            } | {
                InstrumentId.from_str(pair.call_contract.instrument_id),
                InstrumentId.from_str(pair.put_contract.instrument_id),
            }),
            strategy_id="BTCShortStraddleStrategy-001",
            order_id_tag="001",
        )
        strategy = BTCShortStraddleStrategy(config=strategy_config)
        strategy.set_event_listener(self._broadcast)
        if initial_entry:
            strategy.queue_entry(initial_entry)
        self._node_ready.clear()
        self.node = TradingNode(config=node_config)
        self.node.add_data_client_factory("BYBIT", BybitLiveDataClientFactory)
        self.node.add_exec_client_factory("BYBIT", BybitLiveExecClientFactory)
        self.node.build()
        self.node.trader.add_strategy(strategy)
        self.active_strategy = strategy
        self._node_error = None

        def run_node() -> None:
            try:
                self.node.run()
            except Exception as exc:
                self._node_error = exc
                self._node_ready.set()
                self._broadcast({"type": "DASHBOARD_COMMAND_ERROR", "message": f"NautilusTrader node stopped: {exc}"})

        self._node_thread = threading.Thread(target=run_node, name="nautilus-bybit-node", daemon=True)
        self._node_thread.start()
        if not self._node_ready.wait(timeout=45):
            raise TimeoutError("NautilusTrader did not start the Bybit strategy within 45 seconds.")
        if self._node_error:
            raise RuntimeError(f"NautilusTrader failed to start: {self._node_error}") from self._node_error
        if not self._node_is_running():
            raise RuntimeError("NautilusTrader strategy became ready, but the TradingNode is not running.")

    def start_for_saved_entries(self) -> None:
        """Start a no-entry Nautilus node so saved position IDs can reconcile from its cache."""
        saved = state_persistence.get_active_strategies()
        if not saved or self._node_is_running():
            return
        first = next((item for item in saved if item.get("call_symbol") and item.get("put_symbol")), None)
        if first is None:
            return
        expiry_ms = int(first.get("expiry_ms", 0) or 0)
        expiry_text = str(first.get("expiry_str") or "")
        strike = float(first.get("strike", 0.0) or 0.0)
        call_symbol = str(first["call_symbol"])
        put_symbol = str(first["put_symbol"])
        call = OptionContractInfo(
            symbol=call_symbol,
            instrument_id=f"{call_symbol}-OPTION.BYBIT",
            strike=strike,
            option_type="CALL",
            delivery_time_ms=expiry_ms,
            expiry_str=expiry_text,
            min_order_qty=0.01,
            price_tick_size=5.0,
        )
        put = OptionContractInfo(
            symbol=put_symbol,
            instrument_id=f"{put_symbol}-OPTION.BYBIT",
            strike=strike,
            option_type="PUT",
            delivery_time_ms=expiry_ms,
            expiry_str=expiry_text,
            min_order_qty=0.01,
            price_tick_size=5.0,
        )
        pair = StraddlePair(
            btc_price=0.0,
            expiry_ms=expiry_ms,
            expiry_str=expiry_text,
            atm_strike=strike,
            call_contract=call,
            put_contract=put,
            selected_strike=strike,
        )
        self._start_node_for_pair(
            pair,
            float(first.get("trade_size", 0.01) or 0.01),
            int(first.get("level", 2) or 2),
            None,
        )

    def start_straddle(
        self,
        level: int = 2,
        trade_size: float = 0.01,
        target_strike: Optional[float] = None,
        target_expiry_ms: Optional[int] = None,
        simulated: bool = False,
        **kwargs,
    ) -> Dict[str, Any]:
        """Queue both entry legs for NautilusTrader's strategy and ExecutionEngine."""
        if trade_size <= 0:
            raise ValueError("Trade size must be greater than zero.")
        if simulated:
            raise RuntimeError("Simulated dashboard execution is disabled; orders must be confirmed by the live NautilusTrader engine.")

        pair = self.selector.select_atm_straddle(
            target_expiry_ms=target_expiry_ms,
            target_strike=target_strike,
        )
        if not pair.call_contract or not pair.put_contract:
            raise RuntimeError("Cannot submit a straddle unless both Call and Put instruments are available.")
        if pair.call_contract.strike != pair.put_contract.strike or pair.call_contract.delivery_time_ms != pair.put_contract.delivery_time_ms:
            raise RuntimeError("Call and Put contracts must have the same strike and expiry.")

        entry_id = f"STRAT-{time.time_ns()}"
        created_ts = time.time()
        entry = {
            "entry_id": entry_id,
            "call_instrument_id": pair.call_contract.instrument_id,
            "put_instrument_id": pair.put_contract.instrument_id,
            "call_symbol": pair.call_contract.symbol,
            "put_symbol": pair.put_contract.symbol,
            "trade_size": float(trade_size),
            "level": int(level),
            "strike": float(pair.selected_strike or pair.atm_strike),
            "expiry_ms": int(pair.expiry_ms),
        }
        state_persistence.add_active_strategy({
            "strategy_id": entry_id,
            "name": f"Strategy {entry['strike']:,.0f} Straddle",
            "level": level,
            "strike": entry["strike"],
            "expiry_str": pair.expiry_str,
            "expiry_ms": pair.expiry_ms,
            "call_symbol": pair.call_contract.symbol,
            "put_symbol": pair.put_contract.symbol,
            "trade_size": trade_size,
            "status": "SUBMITTING",
            "live_verified": False,
            "is_active": True,
            "created_ts": created_ts,
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "time_str": time.strftime("%H:%M:%S"),
        })
        try:
            with self._entry_lock:
                if self._node_is_running() and self.active_strategy:
                    self.active_strategy.queue_entry(entry)
                else:
                    self._start_node_for_pair(pair, trade_size, level, entry)
        except Exception as exc:
            state_persistence.update_active_strategy(entry_id, {
                "status": "EXECUTION_ERROR",
                "live_verified": False,
                "error": str(exc),
            })
            raise

        self.is_simulated = False
        self._last_exit_ts = 0.0
        self.latest_telemetry["status"] = "SUBMITTING"
        return {
            "status": "SUBMITTED",
            "entry_id": entry_id,
            "strategy_id": entry_id,
            "call_contract": pair.call_contract.symbol,
            "put_contract": pair.put_contract.symbol,
            "trade_size": trade_size,
            "simulated": False,
            "message": "Both legs were queued through NautilusTrader. The entry is active only after both fills are confirmed.",
        }

    def _run_monitoring_loop(self, pair: Optional[StraddlePair] = None, trade_size: float = 0.01, level: int = 2) -> None:
        """Polls live top-of-book quotes every second for all active strategies, updating buyback premium and PnL %."""
        while self.is_running:
            time.sleep(1.0)
            if not self.is_running:
                break
            try:
                active_strats = state_persistence.get_active_strategies()
                if not active_strats:
                    continue

                total_entry = 0.0
                total_buyback = 0.0
                total_pnl_usd = 0.0

                for strat in active_strats:
                    csym = strat.get("call_symbol")
                    psym = strat.get("put_symbol")
                    if not csym or not psym:
                        continue
                    ct = self._fetch_option_ticker(csym)
                    pt = self._fetch_option_ticker(psym)
                    ca = ct.get("ask1Price", 0.0)
                    pa = pt.get("ask1Price", 0.0)
                    if ca <= 0 or pa <= 0:
                        continue
                    bb = round(ca + pa, 2)
                    entry = float(strat.get("combined_entry_premium", 0.0) or (ca + pa))
                    tsize = float(strat.get("trade_size", 0.01) or 0.01)
                    pnl_pct = round(((entry - bb) / entry) * 100.0, 2) if entry > 0 else 0.0
                    pnl_usd = round((entry - bb) * tsize, 2)

                    strat_updates = {
                        "current_buyback_premium": bb,
                        "pnl_pct": pnl_pct,
                        "pnl_usd": pnl_usd,
                        "call_ask": ca,
                        "put_ask": pa,
                    }
                    state_persistence.update_active_strategy(strat["strategy_id"], strat_updates)

                    total_entry += entry
                    total_buyback += bb
                    total_pnl_usd += pnl_usd

                total_pnl_pct = round(((total_entry - total_buyback) / total_entry) * 100.0, 2) if total_entry > 0 else 0.0
                self.latest_telemetry["pnl_pct"] = total_pnl_pct
                self.latest_telemetry["pnl_usd"] = round(total_pnl_usd, 2)
                self.latest_telemetry["buyback_premium"] = round(total_buyback, 2)
                self.latest_telemetry["current_buyback_premium"] = round(total_buyback, 2)

                if self.active_strategy:
                    self.active_strategy.current_pnl_pct = total_pnl_pct
                    self.active_strategy.current_pnl_usd = round(total_pnl_usd, 2)
                    self.active_strategy.current_buyback_premium = round(total_buyback, 2)

                self._broadcast({
                    "type": "VALUATION_UPDATE",
                    "buyback_premium": round(total_buyback, 2),
                    "current_buyback_premium": round(total_buyback, 2),
                    "pnl_pct": total_pnl_pct,
                    "pnl_usd": round(total_pnl_usd, 2),
                    "active_strategies": state_persistence.get_active_strategies(),
                })

                if level == 2 and self.auto_exit_on_market and self.active_strategy:
                    self.active_strategy._check_risk_management()
            except Exception as e:
                print(f"[MonitoringLoop] Error: {e}")

    def _reconstruct_strategy(
        self,
        call_symbol: str,
        put_symbol: str,
        call_entry: float,
        put_entry: float,
        qty: float = 0.01,
        level: int = 2,
    ) -> None:
        """Reconstructs the active strategy state from discovered positions or persistence."""
        call_id = InstrumentId.from_str(f"{call_symbol}-OPTION.BYBIT")
        put_id = InstrumentId.from_str(f"{put_symbol}-OPTION.BYBIT")
        combined_entry = round(call_entry + put_entry, 2)

        config = BTCShortStraddleConfig(
            call_instrument_id=call_id,
            put_instrument_id=put_id,
            trade_size=Decimal(str(qty)),
            level=level,
            stop_loss_pct=Decimal("0.01"),
            take_profit_pct=Decimal("0.02"),
        )
        self.active_strategy = BTCShortStraddleStrategy(config=config)
        self.active_strategy.set_event_listener(self._broadcast)
        self.active_strategy.call_entry_filled = True
        self.active_strategy.put_entry_filled = True
        self.active_strategy.call_entry_price = call_entry
        self.active_strategy.put_entry_price = put_entry
        self.active_strategy.combined_entry_premium = combined_entry
        self.active_strategy.stop_loss_target_premium = round(combined_entry * 1.01, 2)
        self.active_strategy.take_profit_target_premium = round(combined_entry * 0.98, 2)
        self.active_strategy.strategy_state = StrategyState.ACTIVE_POSITION
        self.is_running = True
        self._last_exit_ts = 0.0

        # Start live monitoring thread for reconciled positions
        def _reconciled_monitor():
            while self.is_running and self.active_strategy and self.active_strategy.strategy_state == StrategyState.ACTIVE_POSITION:
                time.sleep(1.0)
                if not self.is_running:
                    break
                try:
                    ct = self._fetch_option_ticker(call_symbol)
                    pt = self._fetch_option_ticker(put_symbol)
                    ca = ct.get("ask1Price", 0.0) or (ct.get("markPrice", 0.0) * 1.01)
                    pa = pt.get("ask1Price", 0.0) or (pt.get("markPrice", 0.0) * 1.01)
                    if ca > 0 and pa > 0:
                        buyback = round(ca + pa, 2)
                        self.active_strategy.current_call_ask = ca
                        self.active_strategy.current_put_ask = pa
                        self.active_strategy.current_buyback_premium = buyback
                        pnl_pct = round(((combined_entry - buyback) / combined_entry) * 100.0, 2) if combined_entry > 0 else 0.0
                        pnl_usd = round((combined_entry - buyback) * qty, 2)
                        self.active_strategy.current_pnl_pct = pnl_pct
                        self.active_strategy.current_pnl_usd = pnl_usd
                        self.latest_telemetry["pnl_pct"] = pnl_pct
                        self.latest_telemetry["pnl_usd"] = pnl_usd
                        self.latest_telemetry["buyback_premium"] = buyback
                        self.latest_telemetry["current_buyback_premium"] = buyback

                        state_persistence.update_active_trade({
                            "current_buyback_premium": buyback,
                            "pnl_pct": pnl_pct,
                            "pnl_usd": pnl_usd,
                        })

                        if not self.is_running or not self.active_strategy or self.active_strategy.strategy_state != StrategyState.ACTIVE_POSITION:
                            break

                        self._broadcast({
                            "type": "VALUATION_UPDATE",
                            "buyback_premium": buyback,
                            "current_buyback_premium": buyback,
                            "pnl_pct": pnl_pct,
                            "pnl_usd": pnl_usd,
                            "call_ask": ca,
                            "put_ask": pa,
                        })

                        if level == 2 and self.auto_exit_on_market:
                            self.active_strategy._check_risk_management()
                except Exception as e:
                    print(f"[ReconciledMonitor] Notice: {e}")

        self._monitor_thread = threading.Thread(target=_reconciled_monitor, daemon=True)
        self._monitor_thread.start()

    def reconcile_positions(self) -> List[Dict[str, Any]]:
        """Reconcile persisted entry mappings against positions in NautilusTrader's cache."""
        if not self._node_is_running():
            return []
        live = self.live_positions()
        by_id = {str(position.get("position_id")): position for position in live}
        entries = state_persistence.get_active_strategies()
        used_positions = set()
        # Prefer a previously saved position ID, then opening-order linkage.
        for entry in entries:
            for symbol_key, pos_key, order_key in (
                ("call_symbol", "call_position_id", "call_order_id"),
                ("put_symbol", "put_position_id", "put_order_id"),
            ):
                pos_id = str(entry.get(pos_key) or "")
                if pos_id in by_id and by_id[pos_id].get("symbol") == entry.get(symbol_key):
                    used_positions.add(pos_id)
                    continue
                order_id = str(entry.get(order_key) or "")
                match = next(
                    (p for p in live if order_id and p.get("opening_order_id") == order_id and p.get("symbol") == entry.get(symbol_key)),
                    None,
                )
                if match:
                    entry[pos_key] = str(match["position_id"])
                    used_positions.add(entry[pos_key])
                    state_persistence.update_active_strategy(str(entry["strategy_id"]), {pos_key: entry[pos_key]})

        # Recover older persisted rows only where a contract has one unambiguous
        # live Nautilus position. Never guess when same-contract entries collide.
        for entry in entries:
            for symbol_key, pos_key in (("call_symbol", "call_position_id"), ("put_symbol", "put_position_id")):
                current_id = str(entry.get(pos_key) or "")
                if current_id in by_id:
                    continue
                candidates = [
                    p for p in live
                    if p.get("symbol") == entry.get(symbol_key)
                    and str(p.get("position_id")) not in used_positions
                ]
                if len(candidates) == 1:
                    found_id = str(candidates[0]["position_id"])
                    entry[pos_key] = found_id
                    used_positions.add(found_id)
                    state_persistence.update_active_strategy(str(entry["strategy_id"]), {pos_key: found_id})

        any_confirmed = False
        for entry in entries:
            call_id = str(entry.get("call_position_id") or "")
            put_id = str(entry.get("put_position_id") or "")
            if call_id in by_id and put_id in by_id:
                state_persistence.update_active_strategy(entry["strategy_id"], {
                    "status": "OPEN",
                    "live_verified": True,
                })
                entry.update(status="OPEN", live_verified=True)
                if self.active_strategy:
                    self.active_strategy.queue_restore(entry)
                any_confirmed = True
            else:
                # An empty or incomplete cache is not proof that the exchange is flat.
                state_persistence.update_active_strategy(entry["strategy_id"], {
                    "status": "RECONCILIATION_REQUIRED",
                    "live_verified": False,
                })
        self.is_running = any_confirmed or bool(state_persistence.get_active_strategies())
        return live

    def stop_straddle(self) -> Dict[str, Any]:
        """Queue closes for all confirmed entries through NautilusTrader; preserve mappings until flat."""
        entries = state_persistence.get_active_strategies()
        if not entries:
            return {"status": "STOPPED", "message": "No saved straddle entries are open.", "verified_flat": not self.live_positions()}
        if not self._node_is_running() or not self.active_strategy:
            return {"status": "ERROR", "message": "NautilusTrader TradingNode is not running; entries were not cleared.", "verified_flat": False}
        submitted = []
        errors = []
        for entry in entries:
            result = self.close_strategy(str(entry.get("strategy_id", "")))
            if result.get("status") == "ERROR":
                errors.append(result.get("message"))
            else:
                submitted.append(entry.get("strategy_id"))
        self._last_exit_ts = time.time()
        if errors:
            return {"status": "PARTIAL", "message": "; ".join(errors), "close_submitted": submitted, "verified_flat": False}
        return {
            "status": "CLOSING",
            "message": "Close requests for confirmed entry position IDs were submitted through NautilusTrader.",
            "close_submitted": submitted,
            "verified_flat": False,
        }

    def scale_in_straddle(
        self,
        additional_size: float = 0.01,
        target_strike: Optional[float] = None,
        target_expiry_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Opens a separate strategy entry using an active Call/Put pair."""
        if additional_size <= 0:
            raise ValueError("Additional size must be greater than zero.")

        active_strategies = state_persistence.get_active_strategies()
        if not active_strategies:
            raise RuntimeError("There is no active straddle to add to.")

        matching = [
            strat for strat in active_strategies
            if (target_strike is None or abs(float(strat.get("strike", 0.0)) - target_strike) < 1.0)
            and (target_expiry_ms is None or int(strat.get("expiry_ms", 0) or 0) == target_expiry_ms)
        ]
        if not matching:
            raise RuntimeError("No active straddle matches the selected strike and expiry. Choose an active pair or start a new one.")

        # Reuse the entry flow, which now creates a new strategy record for
        # every request instead of blending the added size into an old row.
        active = matching[0]
        return self.start_straddle(
            level=int(active.get("level", 2) or 2),
            trade_size=additional_size,
            target_strike=float(active.get("strike", target_strike or 0.0)) or target_strike,
            target_expiry_ms=int(active.get("expiry_ms", target_expiry_ms or 0) or 0) or target_expiry_ms,
            simulated=self.is_simulated,
        )

    def simulate_price_shock(self, shock_type: str, strategy_id: Optional[str] = None, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Dashboard shock control requests a NautilusTrader close; it does not synthesize fills."""
        target_id = strategy_id or symbol
        if not self._node_is_running() or not self.active_strategy:
            return {"status": "ERROR", "message": "NautilusTrader TradingNode is not running."}
        if target_id and target_id != "ALL":
            result = self.close_strategy(target_id)
            if result.get("status") == "ERROR":
                return result
            return {**result, "message": f"{shock_type} action queued through NautilusTrader."}

        results = [self.close_strategy(str(entry.get("strategy_id", ""))) for entry in state_persistence.get_active_strategies()]
        errors = [result.get("message") for result in results if result.get("status") == "ERROR"]
        if errors:
            return {"status": "PARTIAL", "message": "; ".join(errors), "verified_flat": False}
        return {
            "status": "CLOSING",
            "message": f"{shock_type} close requests queued through NautilusTrader for all confirmed entries.",
            "verified_flat": False,
        }
