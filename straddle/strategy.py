"""
Official NautilusTrader BTC Options Short Straddle Strategy.

Implements an automated At-The-Money (ATM) Bitcoin Short Straddle using
native NautilusTrader execution primitives, portfolio tracking, and event handlers.
"""

from decimal import Decimal
from enum import Enum
import time
from typing import Any, Callable, Dict, List, Optional

from nautilus_trader.common.enums import LogColor
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.events import OrderFilled, OrderRejected, OrderCanceled
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId
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

        # 2. Check if positions are already open on Bybit (reconciled on node startup)
        if not self.portfolio.is_flat(self.call_instrument_id) and not self.portfolio.is_flat(self.put_instrument_id):
            self.log.info("Active straddle positions detected on Bybit. Resuming monitoring loop.", LogColor.GREEN)
            self.strategy_state = StrategyState.ACTIVE_POSITION
            return

        # 3. Enter straddle by submitting simultaneous market sell orders
        self.enter_straddle()

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
        if event.instrument_id == self.call_instrument_id:
            self.call_entry_filled = True
            self.call_entry_price = event.last_px.as_double()
        elif event.instrument_id == self.put_instrument_id:
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

    def on_quote_tick(self, tick: QuoteTick) -> None:
        """Updates ask prices and continuously evaluates risk targets."""
        if tick.instrument_id == self.call_instrument_id:
            self.current_call_ask = tick.ask_price.as_double()
        elif tick.instrument_id == self.put_instrument_id:
            self.current_put_ask = tick.ask_price.as_double()

        if self.strategy_state == StrategyState.ACTIVE_POSITION and self.current_call_ask > 0 and self.current_put_ask > 0:
            self.current_buyback_premium = self.current_call_ask + self.current_put_ask
            if self.combined_entry_premium > 0:
                self.current_pnl_pct = ((self.combined_entry_premium - self.current_buyback_premium) / self.combined_entry_premium) * 100.0
                self.current_pnl_usd = (self.combined_entry_premium - self.current_buyback_premium) * float(self.config.trade_size)

            if self.config.level == 2:
                self._check_risk_management()

    def _check_risk_management(self) -> None:
        """Evaluates automated 1% SL and 2% TP conditions (idempotent)."""
        if self.exit_triggered or self.strategy_state != StrategyState.ACTIVE_POSITION:
            return

        if self.current_buyback_premium >= self.stop_loss_target_premium:
            self.log.warn(f"STOP-LOSS HIT: Buyback ${self.current_buyback_premium:.2f} >= ${self.stop_loss_target_premium:.2f}")
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
            self.close_all_positions(self.call_instrument_id)
            self.close_all_positions(self.put_instrument_id)
            self.stop()

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
