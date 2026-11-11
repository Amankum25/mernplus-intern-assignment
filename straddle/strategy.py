"""
================================================================================
⚡ OFFICIAL NAUTILUSTRADER BTC SHORT STRADDLE STRATEGY (straddle/strategy.py)
================================================================================
Institutional At-The-Money (ATM) Bitcoin Short Straddle strategy implemented on
NautilusTrader's event-driven framework and connected to Bybit V5 Options.

Core Concepts for Interview:
1. Short Straddle Mechanics:
   - Sells 1 ATM Call + Sells 1 ATM Put at the identical strike and expiry date.
   - Profit Source: Time decay (Theta) and volatility contraction (Vega).
   - Peak Profit: Combined entry premium received (V_entry = P_call + P_put).
   - Tail Risk: Unhedged delta if the market makes a violent directional breakout.
2. Level 2 Risk Management (1% Stop-Loss / 2% Take-Profit):
   - Bybit V5 Options do NOT support exchange-side STOP_MARKET / STOP_LIMIT orders.
   - Therefore, risk must be monitored client-side in the event loop (on_quote_tick).
   - Valuation is evaluated on the ASK price: V_buyback = P_call_ask + P_put_ask.
   - If V_buyback >= 1.01 * V_entry -> Triggers Stop-Loss (+1% buyback increase).
   - If V_buyback <= 0.98 * V_entry -> Triggers Take-Profit (-2% buyback decay).
3. Legging Risk Protocol:
   - Entering a two-leg trade with independent market orders risks asymmetric execution.
   - If one leg fills while the other times out or gets rejected, the strategy immediately
     cancels the pending leg and unwinds the filled leg via Market Buy (reduce_only=True)
     to avoid naked directional exposure.
4. Idempotency & Thread Safety:
   - Atomic state transitions (exit_triggered = True) prevent fast subsequent market ticks
     from firing duplicate exit orders.
================================================================================
"""

from decimal import Decimal
from enum import Enum
import queue
import time
from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional

from nautilus_trader.common.enums import LogColor
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.events import OrderFilled, OrderRejected, OrderCanceled, PositionClosed, PositionOpened
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId, PositionId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.trading.strategy import Strategy

from straddle.config import BTCShortStraddleConfig


class StrategyState(str, Enum):
    INITIALIZING = "INITIALIZING"
    WAITING_FOR_DATA = "WAITING_FOR_DATA"
    SUBMITTING_ENTRY = "SUBMITTING_ENTRY"
    ACTIVE_POSITION = "ACTIVE_POSITION"
    EXIT_TRIGGERED = "EXIT_TRIGGERED"
    CLOSED = "CLOSED"
    UNHEDGED_ALERT = "UNHEDGED_ALERT"


class BTCShortStraddleStrategy(Strategy):
    """
    Institutional BTC Options ATM Short Straddle Strategy for NautilusTrader.

    Trading Path:
    - on_start(): Subscribes to live quotes. If not already positioned, submits
      simultaneous Market Sell orders for ATM Call and Put legs.
    - on_order_filled(): Records execution prices and calculates 1% SL / 2% TP targets.
    - on_quote_tick(): Evaluates ask-side buyback cost against SL/TP thresholds.
    - _exit_straddle(): Exits via native self.close_all_positions() and stops cleanly.
    """

    def __init__(self, config: BTCShortStraddleConfig) -> None:
        super().__init__(config)
        self.call_instrument_id: InstrumentId = config.call_instrument_id
        self.put_instrument_id: InstrumentId = config.put_instrument_id

        self.strategy_state: StrategyState = StrategyState.INITIALIZING
        self.exit_triggered: bool = False

        # Order tracking
        self.call_entry_order_id: Optional[ClientOrderId] = None
        self.put_entry_order_id: Optional[ClientOrderId] = None
        self.call_entry_filled: bool = False
        self.put_entry_filled: bool = False
        self.call_entry_price: float = 0.0
        self.put_entry_price: float = 0.0

        # Valuation & Risk Management
        self.combined_entry_premium: float = 0.0
        self.stop_loss_target_premium: float = 0.0
        self.take_profit_target_premium: float = 0.0

        self.current_call_ask: float = 0.0
        self.current_put_ask: float = 0.0
        self.current_buyback_premium: float = 0.0
        self.current_pnl_pct: float = 0.0
        self.current_pnl_usd: float = 0.0

        # Event notification hook
        self.event_listener: Optional[Callable[[Dict[str, Any]], None]] = None
        self._dashboard_commands: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self._dashboard_entries: Dict[str, Dict[str, Any]] = {}
        self._restored_entry_ids: set[str] = set()
        self._dashboard_order_context: Dict[ClientOrderId, tuple[str, str]] = {}
        self._subscribed_instruments = {self.call_instrument_id, self.put_instrument_id}
        self._commands_timer_name = "dashboard-straddle-commands"

    def set_event_listener(self, listener: Callable[[Dict[str, Any]], None]) -> None:
        self.event_listener = listener

    def _emit(self, event_type: str, data: Dict[str, Any]) -> None:
        if self.event_listener:
            try:
                self.event_listener({"type": event_type, "timestamp": time.time(), "state": self.strategy_state.value if isinstance(self.strategy_state, StrategyState) else str(self.strategy_state), **data})
            except Exception:
                pass

    def on_start(self) -> None:
        """Called by Nautilus when the engine is running and reconciled."""
        self.log.info(f"Starting BTC Short Straddle Strategy [Level {self.config.level}]", LogColor.BLUE)

        # 1. Subscribe to live quotes for Call and Put legs
        self.subscribe_quote_ticks(self.call_instrument_id)
        self.subscribe_quote_ticks(self.put_instrument_id)
        self.strategy_state = StrategyState.WAITING_FOR_DATA
        self.clock.set_timer(
            name=self._commands_timer_name,
            interval=timedelta(milliseconds=100),
            callback=self._process_dashboard_commands,
        )
        self._emit("NAUTILUS_STRATEGY_READY", {})
        if self.config.auto_start_entry:
            self.enter_straddle()

    def queue_entry(self, entry: Dict[str, Any]) -> None:
        """Queue a dashboard entry so order submission runs on Nautilus' strategy clock."""
        self._dashboard_commands.put({"type": "ENTRY", "entry": dict(entry)})

    def queue_close(self, entry_id: str, position_ids: Optional[Dict[str, str]] = None) -> None:
        self._dashboard_commands.put({
            "type": "CLOSE",
            "entry_id": str(entry_id),
            "position_ids": dict(position_ids or {}),
        })

    def queue_restore(self, entry: Dict[str, Any]) -> None:
        """Restore a persisted mapping after NautilusTrader confirms its live positions."""
        self._dashboard_commands.put({"type": "RESTORE", "entry": dict(entry)})

    def queue_cancel(self, client_order_id: str) -> None:
        self._dashboard_commands.put({"type": "CANCEL", "client_order_id": str(client_order_id)})

    def _process_dashboard_commands(self, _event=None) -> None:
        while True:
            try:
                command = self._dashboard_commands.get_nowait()
            except queue.Empty:
                return
            try:
                kind = command.get("type")
                if kind == "ENTRY":
                    self._submit_dashboard_entry(command["entry"])
                elif kind == "CLOSE":
                    self._close_dashboard_entry(command["entry_id"], command.get("position_ids"))
                elif kind == "RESTORE":
                    self._restore_dashboard_entry(command["entry"])
                elif kind == "CANCEL":
                    self._cancel_dashboard_order(command["client_order_id"])
            except Exception as exc:
                self.log.exception(f"Dashboard command failed: {exc}")
                self._emit("DASHBOARD_COMMAND_ERROR", {"message": str(exc), **command})

    def _submit_dashboard_entry(self, entry: Dict[str, Any]) -> None:
        entry_id = str(entry["entry_id"])
        call_id = InstrumentId.from_str(str(entry["call_instrument_id"]))
        put_id = InstrumentId.from_str(str(entry["put_instrument_id"]))
        qty = Quantity.from_str(str(entry["trade_size"]))
        for instrument_id in (call_id, put_id):
            if instrument_id not in self._subscribed_instruments:
                self.subscribe_quote_ticks(instrument_id)
                self._subscribed_instruments.add(instrument_id)

        entry_state = {
            **entry,
            "call_instrument_id": call_id,
            "put_instrument_id": put_id,
            "status": "SUBMITTING",
            "call_filled": False,
            "put_filled": False,
            "filled_quantities": {"CALL": 0.0, "PUT": 0.0},
            "requested_quantity": float(entry["trade_size"]),
            "position_ids": {},
            "fill_prices": {},
            "order_ids": {},
            "exit_triggered": False,
            "established_event_emitted": False,
        }
        self._dashboard_entries[entry_id] = entry_state
        call_order = self.order_factory.market(
            instrument_id=call_id,
            order_side=OrderSide.SELL,
            quantity=qty,
            time_in_force=TimeInForce.IOC,
            tags=[entry_id, "CALL"],
        )
        put_order = self.order_factory.market(
            instrument_id=put_id,
            order_side=OrderSide.SELL,
            quantity=qty,
            time_in_force=TimeInForce.IOC,
            tags=[entry_id, "PUT"],
        )
        entry_state["order_ids"] = {"CALL": str(call_order.client_order_id), "PUT": str(put_order.client_order_id)}
        self._dashboard_order_context[call_order.client_order_id] = (entry_id, "CALL")
        self._dashboard_order_context[put_order.client_order_id] = (entry_id, "PUT")
        self._emit("ENTRY_SUBMITTED", {
            "entry_id": entry_id,
            "call_order_id": str(call_order.client_order_id),
            "put_order_id": str(put_order.client_order_id),
            "call_symbol": str(call_id).split("-OPTION")[0],
            "put_symbol": str(put_id).split("-OPTION")[0],
            "trade_size": float(entry["trade_size"]),
        })
        self.submit_order(call_order)
        self.submit_order(put_order)
        self.clock.set_timer(
            name=f"entry-timeout-{entry_id}",
            interval=timedelta(seconds=float(self.config.entry_timeout_secs)),
            callback=lambda _evt, key=entry_id: self._handle_dashboard_entry_timeout(key),
            fire_immediately=False,
        )

    def _cancel_dashboard_order(self, client_order_id: str) -> None:
        order = self.cache.order(ClientOrderId(client_order_id))
        if order is not None and not order.is_closed:
            self.cancel_order(order)

    def _restore_dashboard_entry(self, saved: Dict[str, Any]) -> None:
        entry_id = str(saved.get("strategy_id", ""))
        if not entry_id or entry_id in self._restored_entry_ids:
            return
        call_id = InstrumentId.from_str(f"{saved['call_symbol']}-OPTION.BYBIT")
        put_id = InstrumentId.from_str(f"{saved['put_symbol']}-OPTION.BYBIT")
        position_ids = {"CALL": saved.get("call_position_id"), "PUT": saved.get("put_position_id")}
        if not all(position_ids.values()):
            return
        self._dashboard_entries[entry_id] = {
            **saved,
            "entry_id": entry_id,
            "call_instrument_id": call_id,
            "put_instrument_id": put_id,
            "position_ids": position_ids,
            "fill_prices": {
                "CALL": float(saved.get("call_entry", 0.0) or 0.0),
                "PUT": float(saved.get("put_entry", 0.0) or 0.0),
            },
            "order_ids": {
                "CALL": saved.get("call_order_id"),
                "PUT": saved.get("put_order_id"),
            },
            "call_filled": True,
            "put_filled": True,
            "status": "OPEN",
            "exit_triggered": False,
            "closed_positions": set(),
        }
        self.subscribe_quote_ticks(call_id)
        self.subscribe_quote_ticks(put_id)
        self._subscribed_instruments.update((call_id, put_id))
        for leg in ("CALL", "PUT"):
            order_id = (saved.get("call_order_id") if leg == "CALL" else saved.get("put_order_id"))
            if order_id:
                self._dashboard_order_context.setdefault(ClientOrderId(str(order_id)), (entry_id, leg))
        self._restored_entry_ids.add(entry_id)

    def _close_dashboard_entry(self, entry_id: str, position_ids: Optional[Dict[str, str]] = None) -> None:
        entry = self._dashboard_entries.get(entry_id)
        targets = position_ids or (entry.get("position_ids", {}) if entry else {})
        if not targets:
            self._emit("DASHBOARD_COMMAND_ERROR", {"entry_id": entry_id, "message": "No confirmed NautilusTrader position IDs are mapped to this entry."})
            return
        if entry:
            entry["exit_triggered"] = True
            entry["status"] = "CLOSING"
        for raw_position_id in targets.values():
            if not raw_position_id:
                continue
            position = self.cache.position(PositionId(str(raw_position_id)))
            if position is not None and position.is_open:
                self.close_position(position)
        self._emit("ENTRY_CLOSE_SUBMITTED", {"entry_id": entry_id, "position_ids": targets})

    def _handle_dashboard_entry_timeout(self, entry_id: str) -> None:
        entry = self._dashboard_entries.get(entry_id)
        if not entry or entry.get("status") != "SUBMITTING":
            return
        self.clock.cancel_timer(f"entry-timeout-{entry_id}")
        for leg, client_order_id in entry.get("order_ids", {}).items():
            if not entry.get("position_ids", {}).get(leg):
                order = self.cache.order(ClientOrderId(client_order_id))
                if order is not None:
                    for position in self.cache.positions_open(instrument_id=order.instrument_id):
                        if str(position.opening_order_id) == str(client_order_id):
                            entry.setdefault("position_ids", {})[leg] = str(position.id)
                            break
            if not entry.get("position_ids", {}).get(leg):
                self._cancel_dashboard_order(client_order_id)
        if entry.get("position_ids"):
            entry["status"] = "UNHEDGED_ALERT"
            self._close_dashboard_entry(entry_id)
        else:
            entry["status"] = "REJECTED"
        self._emit("ENTRY_TIMEOUT", {
            "entry_id": entry_id,
            "status": entry["status"],
            "position_ids": dict(entry.get("position_ids", {})),
        })

    def enter_straddle(self) -> None:
        """Submits simultaneous Sell market orders for Call and Put legs."""
        if self.strategy_state not in (StrategyState.WAITING_FOR_DATA, StrategyState.INITIALIZING):
            return

        self.strategy_state = StrategyState.SUBMITTING_ENTRY
        qty = Quantity.from_str(str(self.config.trade_size))

        call_order: MarketOrder = self.order_factory.market(
            instrument_id=self.call_instrument_id,
            order_side=OrderSide.SELL,
            quantity=qty,
            time_in_force=TimeInForce.GTC,
        )
        put_order: MarketOrder = self.order_factory.market(
            instrument_id=self.put_instrument_id,
            order_side=OrderSide.SELL,
            quantity=qty,
            time_in_force=TimeInForce.GTC,
        )

        self.call_entry_order_id = call_order.client_order_id
        self.put_entry_order_id = put_order.client_order_id

        self.submit_order(call_order)
        self.submit_order(put_order)
        self.log.info(f"Submitted Straddle Entry Orders: {call_order.client_order_id} & {put_order.client_order_id}", LogColor.CYAN)
        self._emit("ENTRY_SUBMITTED", {"call_order_id": str(call_order.client_order_id), "put_order_id": str(put_order.client_order_id)})

    def on_order_filled(self, event: OrderFilled) -> None:
        """Records fill prices and calculates Level 2 SL/TP targets."""
        context = self._dashboard_order_context.get(event.client_order_id)
        if context is None and event.client_order_id not in (self.call_entry_order_id, self.put_entry_order_id):
            # Record real Nautilus fills for closes and other strategy orders,
            # without misclassifying them as a new entry.
            order = self.cache.order(event.client_order_id)
            self._emit("ORDER_FILLED", {
                "client_order_id": str(event.client_order_id),
                "instrument_id": str(event.instrument_id),
                "position_id": str(event.position_id) if event.position_id else None,
                "side": str(order.side).split(".")[-1] if order is not None else "UNKNOWN",
                "filled_qty": float(order.filled_qty) if order is not None else event.last_qty.as_double(),
                "fill_price": float(order.avg_px) if order is not None and order.avg_px else event.last_px.as_double(),
            })
            return
        if context:
            entry_id, leg = context
            entry = self._dashboard_entries.get(entry_id)
            if entry is None:
                return
            last_qty = event.last_qty.as_double()
            cumulative = entry.setdefault("filled_quantities", {}).get(leg, 0.0) + last_qty
            old_qty = entry["filled_quantities"].get(leg, 0.0)
            old_price = entry.setdefault("fill_prices", {}).get(leg, 0.0)
            entry["filled_quantities"][leg] = cumulative
            entry["fill_prices"][leg] = ((old_qty * old_price) + (last_qty * event.last_px.as_double())) / cumulative if cumulative > 0 else 0.0
            entry.setdefault("position_ids", {})[leg] = str(event.position_id) if event.position_id else None
            entry[f"{leg.lower()}_filled"] = cumulative + 1e-12 >= entry["requested_quantity"]
            self._emit("ORDER_FILLED", {
                "entry_id": entry_id,
                "leg": leg,
                "client_order_id": str(event.client_order_id),
                "position_id": str(event.position_id) if event.position_id else None,
                "instrument_id": str(event.instrument_id),
                "side": "SELL",
                "fill_price": event.last_px.as_double(),
                "filled_qty": cumulative,
            })
            self._maybe_establish_dashboard_entry(entry_id)
            return

        if event.client_order_id == self.call_entry_order_id and event.instrument_id == self.call_instrument_id:
            self.call_entry_filled = True
            self.call_entry_price = event.last_px.as_double()
        elif event.client_order_id == self.put_entry_order_id and event.instrument_id == self.put_instrument_id:
            self.put_entry_filled = True
            self.put_entry_price = event.last_px.as_double()

        if self.call_entry_filled and self.put_entry_filled and self.strategy_state != StrategyState.ACTIVE_POSITION:
            self.combined_entry_premium = self.call_entry_price + self.put_entry_price
            self.stop_loss_target_premium = self.combined_entry_premium * (1.0 + float(self.config.stop_loss_pct))
            self.take_profit_target_premium = self.combined_entry_premium * (1.0 - float(self.config.take_profit_pct))
            self.strategy_state = StrategyState.ACTIVE_POSITION
            self.log.info(
                f"Straddle Active | Entry: ${self.combined_entry_premium:.2f} | "
                f"SL Target: ${self.stop_loss_target_premium:.2f} | "
                f"TP Target: ${self.take_profit_target_premium:.2f}",
                LogColor.YELLOW,
            )
            self._emit("STRADDLE_POSITION_ESTABLISHED", {
                "combined_entry": self.combined_entry_premium,
                "sl_target": self.stop_loss_target_premium,
                "tp_target": self.take_profit_target_premium,
            })

    def on_position_opened(self, event: PositionOpened) -> None:
        """Capture the engine-assigned virtual position ID for each dashboard leg."""
        context = self._dashboard_order_context.get(event.opening_order_id)
        if context is None:
            return
        entry_id, leg = context
        entry = self._dashboard_entries.get(entry_id)
        if entry is None:
            return
        entry.setdefault("position_ids", {})[leg] = str(event.position_id)
        self._maybe_establish_dashboard_entry(entry_id)

    def _maybe_establish_dashboard_entry(self, entry_id: str) -> None:
        entry = self._dashboard_entries.get(entry_id)
        if not entry or entry.get("established_event_emitted"):
            return
        if not (entry.get("call_filled") and entry.get("put_filled")):
            return
        if not all(entry.get("position_ids", {}).get(leg) for leg in ("CALL", "PUT")):
            return
        self.clock.cancel_timer(f"entry-timeout-{entry_id}")
        entry["status"] = "OPEN"
        entry["established_event_emitted"] = True
        call_px = float(entry["fill_prices"]["CALL"])
        put_px = float(entry["fill_prices"]["PUT"])
        combined = call_px + put_px
        entry["combined_entry_premium"] = combined
        entry["stop_loss_target"] = combined * 1.01
        entry["take_profit_target"] = combined * 0.98
        self._emit("STRADDLE_POSITION_ESTABLISHED", {
            "entry_id": entry_id,
            "call_symbol": str(entry["call_instrument_id"]).split("-OPTION")[0],
            "put_symbol": str(entry["put_instrument_id"]).split("-OPTION")[0],
            "call_order_id": entry["order_ids"]["CALL"],
            "put_order_id": entry["order_ids"]["PUT"],
            "call_position_id": entry["position_ids"]["CALL"],
            "put_position_id": entry["position_ids"]["PUT"],
            "call_price": call_px,
            "put_price": put_px,
            "combined_entry": combined,
            "sl_target": entry["stop_loss_target"],
            "tp_target": entry["take_profit_target"],
            "qty": float(entry["trade_size"]),
            "level": int(entry.get("level", self.config.level)),
            "strike": entry.get("strike"),
            "expiry_ms": entry.get("expiry_ms"),
        })

    def on_order_canceled(self, event: OrderCanceled) -> None:
        self._emit("ORDER_STATUS_CHANGED", {
            "client_order_id": str(event.client_order_id),
            "status": "CANCELED",
        })
        self._handle_dashboard_order_failure(event.client_order_id, "CANCELED", "Order canceled")
        context = self._dashboard_order_context.get(event.client_order_id)
        if context:
            self._check_dashboard_entry_terminal(context[0])

    def on_order_rejected(self, event: OrderRejected) -> None:
        self._emit("ORDER_STATUS_CHANGED", {
            "client_order_id": str(event.client_order_id),
            "status": "REJECTED",
            "message": str(event.reason),
        })
        self._handle_dashboard_order_failure(event.client_order_id, "REJECTED", str(event.reason))
        context = self._dashboard_order_context.get(event.client_order_id)
        if context:
            self._check_dashboard_entry_terminal(context[0])

    def _handle_dashboard_order_failure(self, client_order_id: ClientOrderId, status: str, reason: str) -> None:
        context = self._dashboard_order_context.get(client_order_id)
        if not context:
            return
        entry_id, leg = context
        entry = self._dashboard_entries.get(entry_id)
        if not entry or entry.get("status") in ("CLOSED", "CLOSING"):
            return
        entry["status"] = "UNHEDGED_ALERT" if entry.get("position_ids") else status
        other_leg = "PUT" if leg == "CALL" else "CALL"
        other_order_id = entry.get("order_ids", {}).get(other_leg)
        if other_order_id and not entry.get("position_ids", {}).get(other_leg):
            self._cancel_dashboard_order(other_order_id)
        if entry.get("position_ids"):
            self.queue_close(entry_id)
        self._emit("ENTRY_ORDER_FAILED", {
            "entry_id": entry_id,
            "leg": leg,
            "client_order_id": str(client_order_id),
            "status": status,
            "message": reason,
        })

    def _check_dashboard_entry_terminal(self, entry_id: str) -> None:
        entry = self._dashboard_entries.get(entry_id)
        if not entry:
            return
        orders = [
            self.cache.order(ClientOrderId(order_id))
            for order_id in entry.get("order_ids", {}).values()
            if order_id
        ]
        if len(orders) == 2 and all(order is not None and order.is_closed for order in orders):
            if not entry.get("position_ids"):
                entry["status"] = "REJECTED"
                self.clock.cancel_timer(f"entry-timeout-{entry_id}")
                self._emit("ENTRY_ABORTED", {"entry_id": entry_id, "status": "REJECTED"})

    def on_position_closed(self, event: PositionClosed) -> None:
        position_id = str(event.position_id)
        for entry_id, entry in self._dashboard_entries.items():
            for leg, mapped_id in entry.get("position_ids", {}).items():
                if mapped_id == position_id:
                    entry.setdefault("closed_positions", set()).add(leg)
                    self._emit("POSITION_CLOSED", {
                        "entry_id": entry_id,
                        "leg": leg,
                        "position_id": position_id,
                        "realized_pnl": str(event.realized_pnl),
                    })
                    if len(entry["closed_positions"]) >= len([v for v in entry.get("position_ids", {}).values() if v]):
                        entry["status"] = "CLOSED"
                        self._emit("STRATEGY_ENTRY_CLOSED", {"entry_id": entry_id})
                    return

    def on_quote_tick(self, tick: QuoteTick) -> None:
        """Updates ask prices and continuously evaluates risk targets."""
        for entry_id, entry in list(self._dashboard_entries.items()):
            if entry.get("status") != "OPEN":
                continue
            if tick.instrument_id == entry["call_instrument_id"]:
                entry["call_ask"] = tick.ask_price.as_double()
            elif tick.instrument_id == entry["put_instrument_id"]:
                entry["put_ask"] = tick.ask_price.as_double()
            else:
                continue
            if entry.get("call_ask", 0) <= 0 or entry.get("put_ask", 0) <= 0:
                continue
            buyback = entry["call_ask"] + entry["put_ask"]
            combined = float(entry.get("combined_entry_premium", 0.0))
            pnl_pct = ((combined - buyback) / combined) * 100.0 if combined > 0 else 0.0
            pnl_usd = (combined - buyback) * float(entry.get("trade_size", 0.0))
            entry.update(current_buyback_premium=buyback, pnl_pct=pnl_pct, pnl_usd=pnl_usd)
            self._emit("ENTRY_VALUATION_UPDATE", {
                "entry_id": entry_id,
                "buyback_premium": buyback,
                "pnl_pct": pnl_pct,
                "pnl_usd": pnl_usd,
                "call_ask": entry["call_ask"],
                "put_ask": entry["put_ask"],
            })
            if int(entry.get("level", self.config.level)) == 2 and not entry.get("exit_triggered"):
                if buyback >= float(entry.get("stop_loss_target", float("inf"))) or buyback <= float(entry.get("take_profit_target", 0.0)):
                    entry["exit_triggered"] = True
                    self.queue_close(entry_id)

        if tick.instrument_id == self.call_instrument_id:
            self.current_call_ask = tick.ask_price.as_double()
        elif tick.instrument_id == self.put_instrument_id:
            self.current_put_ask = tick.ask_price.as_double()

        if self.strategy_state == StrategyState.ACTIVE_POSITION and self.current_call_ask > 0 and self.current_put_ask > 0:
            self.current_buyback_premium = self.current_call_ask + self.current_put_ask
            if self.combined_entry_premium > 0:
                self.current_pnl_pct = ((self.combined_entry_premium - self.current_buyback_premium) / self.combined_entry_premium) * 100.0
                self.current_pnl_usd = (self.combined_entry_premium - self.current_buyback_premium) * float(self.config.trade_size)

            self._emit("VALUATION_UPDATE", {
                "buyback_premium": self.current_buyback_premium,
                "current_buyback_premium": self.current_buyback_premium,
                "pnl_pct": self.current_pnl_pct,
                "pnl_usd": self.current_pnl_usd,
                "call_ask": self.current_call_ask,
                "put_ask": self.current_put_ask,
            })

            if self.config.level == 2:
                self._check_risk_management()

    def _check_risk_management(self) -> None:
        """Evaluates automated 1% SL and 2% TP conditions (idempotent)."""
        if self.exit_triggered or self.strategy_state != StrategyState.ACTIVE_POSITION:
            return

        if self.current_buyback_premium >= self.stop_loss_target_premium:
            self.log.warning(f"STOP-LOSS HIT: Buyback ${self.current_buyback_premium:.2f} >= ${self.stop_loss_target_premium:.2f}")
            self._exit_straddle("STOP_LOSS_TRIGGERED")
        elif self.current_buyback_premium <= self.take_profit_target_premium:
            self.log.info(f"TAKE-PROFIT HIT: Buyback ${self.current_buyback_premium:.2f} <= ${self.take_profit_target_premium:.2f}", LogColor.GREEN)
            self._exit_straddle("TAKE_PROFIT_TRIGGERED")

    def _exit_straddle(self, reason: str = "MANUAL_EXIT") -> None:
        """Closes both positions simultaneously using native close_all_positions."""
        if self.exit_triggered:
            return
        self.exit_triggered = True
        self.strategy_state = StrategyState.EXIT_TRIGGERED

        # Nautilus close_all_positions generates matching Market Buy orders with reduce_only=True
        if self.trader_id is not None:
            try:
                self.close_all_positions(self.call_instrument_id)
                self.close_all_positions(self.put_instrument_id)
            except Exception:
                pass
            try:
                self.stop()
            except Exception:
                pass

        self.strategy_state = StrategyState.CLOSED
        self._emit(reason, {"buyback_premium": self.current_buyback_premium, "pnl_pct": self.current_pnl_pct})

    def _handle_legging_failure(self, failed_order_id: ClientOrderId) -> None:
        """Unwinds single-leg exposure if the other leg fails to fill."""
        self.strategy_state = StrategyState.UNHEDGED_ALERT
        self.log.error(f"Legging failure on order {failed_order_id}. Unwinding any open positions immediately.")
        if self.trader_id is not None:
            self.close_all_positions(self.call_instrument_id)
            self.close_all_positions(self.put_instrument_id)
            self.stop()

    def on_stop(self) -> None:
        self.log.info("BTC Short Straddle Strategy stopped.", LogColor.YELLOW)


# Alias for clean naming
ATMStraddleStrategy = BTCShortStraddleStrategy
