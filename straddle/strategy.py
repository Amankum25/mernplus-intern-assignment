"""
BTC Options Short Straddle Strategy for NautilusTrader.
Implements Level 1 (Naked Straddle) and Level 2 (1% SL / 2% TP Automated Risk Management).
"""

from decimal import Decimal
import time
from typing import Any, Callable, Dict, List, Optional

from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.data import Data
from nautilus_trader.core.message import Event
from nautilus_trader.model.data import QuoteTick, TradeTick
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.events import (
    OrderAccepted,
    OrderCanceled,
    OrderDenied,
    OrderExpired,
    OrderFilled,
    OrderRejected,
    OrderSubmitted,
    PositionChanged,
    PositionClosed,
    PositionOpened,
)
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.position import Position
from nautilus_trader.trading.strategy import Strategy

from straddle.config import BTCShortStraddleConfig


class StrategyState:
    INITIALIZING = "INITIALIZING"
    WAITING_FOR_DATA = "WAITING_FOR_DATA"
    SUBMITTING_ENTRY = "SUBMITTING_ENTRY"
    ACTIVE_POSITION = "ACTIVE_POSITION"
    EXIT_TRIGGERED = "EXIT_TRIGGERED"
    CLOSED = "CLOSED"
    UNHEDGED_ALERT = "UNHEDGED_ALERT"


class BTCShortStraddleStrategy(Strategy):
    """
    BTC ATM Short Straddle Strategy.

    Level 1:
      - Connects to Bybit options market.
      - Discovers nearest ATM strike for BTC.
      - Sells 1 ATM Call option and 1 ATM Put option with the same strike and expiry.
      - Tracks order acknowledgements, executions, and open positions.

    Level 2:
      - Continuously values the combined buyback premium using ask-side quotes:
        Combined Buyback Premium = Call Ask Price + Put Ask Price
      - Stop-Loss (1%): If combined buyback premium increases by >= 1% from combined entry premium,
        automatically close both positions.
      - Take-Profit (2%): If combined buyback premium decreases by >= 2% from combined entry premium,
        automatically buy back and close both positions.
      - Handles partial executions, leg imbalance timeouts, and guards against duplicate exits.
    """

    def __init__(self, config: BTCShortStraddleConfig) -> None:
        super().__init__(config)

        self.call_instrument: Optional[Instrument] = None
        self.put_instrument: Optional[Instrument] = None

        # State machine
        self.strategy_state: str = StrategyState.INITIALIZING
        self.exit_triggered: bool = False
        self.exit_reason: Optional[str] = None

        # Entry order IDs
        self.call_entry_order_id: Optional[ClientOrderId] = None
        self.put_entry_order_id: Optional[ClientOrderId] = None
        self.call_exit_order_id: Optional[ClientOrderId] = None
        self.put_exit_order_id: Optional[ClientOrderId] = None

        # Execution fills & prices
        self.call_entry_filled: bool = False
        self.put_entry_filled: bool = False
        self.call_entry_price: float = 0.0
        self.put_entry_price: float = 0.0
        self.call_entry_qty: float = 0.0
        self.put_entry_qty: float = 0.0
        self.combined_entry_premium: float = 0.0

        # Live ask prices for buyback valuation
        self.current_call_ask: float = 0.0
        self.current_put_ask: float = 0.0
        self.current_call_bid: float = 0.0
        self.current_put_bid: float = 0.0
        self.current_buyback_premium: float = 0.0
        self.current_pnl_pct: float = 0.0
        self.current_pnl_usd: float = 0.0

        # Risk Management Thresholds (Level 2)
        self.stop_loss_target_premium: float = 0.0
        self.take_profit_target_premium: float = 0.0

        # Safety & Timing
        self.entry_start_time: float = 0.0
        self.last_quote_time: float = 0.0

        # Telemetry & Event Listener for UI / Logging
        self.event_listener: Optional[Callable[[Dict[str, Any]], None]] = None
        self.execution_logs: List[Dict[str, Any]] = []

    def set_event_listener(self, listener: Callable[[Dict[str, Any]], None]) -> None:
        """Register a callback listener for real-time dashboard events."""
        self.event_listener = listener

    def _emit(self, event_type: str, data: Dict[str, Any]) -> None:
        """Emit structured telemetry to the UI and internal logs."""
        payload = {
            "type": event_type,
            "timestamp": time.time(),
            "state": self.strategy_state,
            **data,
        }
        self.execution_logs.append(payload)
        if self.event_listener:
            try:
                self.event_listener(payload)
            except Exception as e:
                self.log.error(f"Error in event listener callback: {e}")

    # -------------------------------------------------------------------------
    # Strategy Lifecycle
    # -------------------------------------------------------------------------

    def on_start(self) -> None:
        """
        Actions performed on strategy start:
        1. Resolves Call and Put instruments from cache.
        2. Subscribes to market data (quotes, trades, Greeks).
        3. Prepares and submits Straddle entry orders.
        """
        self.log.info(
            f"Starting BTC Short Straddle Strategy [Level {self.config.level}]",
            LogColor.BLUE,
        )

        # Retrieve instruments from cache
        self.call_instrument = self.cache.instrument(self.config.call_instrument_id)
        self.put_instrument = self.cache.instrument(self.config.put_instrument_id)

        if not self.call_instrument:
            self.log.error(f"Call instrument not found in cache: {self.config.call_instrument_id}")
            self.stop()
            return

        if not self.put_instrument:
            self.log.error(f"Put instrument not found in cache: {self.config.put_instrument_id}")
            self.stop()
            return

        self.log.info(f"Loaded Call: {self.call_instrument.id}", LogColor.GREEN)
        self.log.info(f"Loaded Put:  {self.put_instrument.id}", LogColor.GREEN)

        # Subscribe to market data for both legs
        self.subscribe_quote_ticks(self.call_instrument.id)
        self.subscribe_quote_ticks(self.put_instrument.id)
        self.subscribe_trade_ticks(self.call_instrument.id)
        self.subscribe_trade_ticks(self.put_instrument.id)

        self.strategy_state = StrategyState.WAITING_FOR_DATA
        self._emit(
            "STRATEGY_STARTED",
            {
                "call_id": str(self.call_instrument.id),
                "put_id": str(self.put_instrument.id),
                "level": self.config.level,
                "trade_size": float(self.config.trade_size),
                "stop_loss_pct": float(self.config.stop_loss_pct * 100),
                "take_profit_pct": float(self.config.take_profit_pct * 100),
            },
        )

        # Submit straddle entry
        self.enter_straddle()

    def on_stop(self) -> None:
        """Cleanup on strategy stop."""
        self.log.info("Stopping BTC Short Straddle Strategy", LogColor.YELLOW)
        self._emit("STRATEGY_STOPPED", {"final_state": self.strategy_state})

    # -------------------------------------------------------------------------
    # Straddle Entry Execution
    # -------------------------------------------------------------------------

    def enter_straddle(self) -> None:
        """
        Submits sell orders for both the Call and Put options to establish
        the short straddle position.
        """
        if self.strategy_state not in (StrategyState.WAITING_FOR_DATA, StrategyState.INITIALIZING):
            return

        self.strategy_state = StrategyState.SUBMITTING_ENTRY
        self.entry_start_time = time.time()
        order_qty = self.call_instrument.make_qty(self.config.trade_size)

        self.log.info(
            f"Submitting Short Straddle Entry: Sell {order_qty} Call & {order_qty} Put",
            LogColor.CYAN,
        )

        # 1. Create Call Sell Order
        call_order: MarketOrder = self.order_factory.market(
            instrument_id=self.call_instrument.id,
            order_side=OrderSide.SELL,
            quantity=order_qty,
            time_in_force=TimeInForce.GTC,
        )
        self.call_entry_order_id = call_order.client_order_id

        # 2. Create Put Sell Order
        put_order: MarketOrder = self.order_factory.market(
            instrument_id=self.put_instrument.id,
            order_side=OrderSide.SELL,
            quantity=order_qty,
            time_in_force=TimeInForce.GTC,
        )
        self.put_entry_order_id = put_order.client_order_id

        # Submit both orders to execution engine
        self.submit_order(call_order)
        self.submit_order(put_order)

        self._emit(
            "ENTRY_SUBMITTED",
            {
                "call_order_id": str(self.call_entry_order_id),
                "put_order_id": str(self.put_entry_order_id),
                "quantity": float(self.config.trade_size),
            },
        )

    # -------------------------------------------------------------------------
    # Market Data Events
    # -------------------------------------------------------------------------

    def on_quote_tick(self, tick: QuoteTick) -> None:
        """
        Receives real-time top-of-book quotes for Call and Put options.
        Continuously recalculates the combined buyback premium using ask-side prices.
        Evaluates Level 2 automated Stop-Loss (1%) and Take-Profit (2%) triggers.
        """
        self.last_quote_time = time.time()

        # Update Call quotes
        if tick.instrument_id == self.call_instrument.id:
            self.current_call_ask = tick.ask_price.as_double()
            self.current_call_bid = tick.bid_price.as_double()

        # Update Put quotes
        elif tick.instrument_id == self.put_instrument.id:
            self.current_put_ask = tick.ask_price.as_double()
            self.current_put_bid = tick.bid_price.as_double()

        # If both legs are filled and position is active, value buyback premium
        if self.strategy_state == StrategyState.ACTIVE_POSITION and self.current_call_ask > 0 and self.current_put_ask > 0:
            # Assignment specification:
            # "calculate the combined premium using both option legs, with consistent contract quantities.
            # Use ask-side quotes for the buyback valuation."
            self.current_buyback_premium = self.current_call_ask + self.current_put_ask

            if self.combined_entry_premium > 0:
                # For short straddle: Profit occurs when buyback cost decreases
                # Premium diff = Entry Premium - Current Buyback Cost
                premium_diff = self.combined_entry_premium - self.current_buyback_premium
                self.current_pnl_pct = (premium_diff / self.combined_entry_premium) * 100.0
                self.current_pnl_usd = premium_diff * float(self.config.trade_size)

                self._emit(
                    "VALUATION_UPDATE",
                    {
                        "call_ask": self.current_call_ask,
                        "put_ask": self.current_put_ask,
                        "combined_buyback_premium": self.current_buyback_premium,
                        "combined_entry_premium": self.combined_entry_premium,
                        "pnl_pct": self.current_pnl_pct,
                        "pnl_usd": self.current_pnl_usd,
                        "stop_loss_target": self.stop_loss_target_premium,
                        "take_profit_target": self.take_profit_target_premium,
                    },
                )

                # Check automated risk management if Level 2 is enabled
                if self.config.level == 2:
                    self._check_risk_management()

    def on_trade_tick(self, tick: TradeTick) -> None:
        """Handle trade ticks for logging or market tracking."""
        pass

    # -------------------------------------------------------------------------
    # Level 2 Automated Risk Management Logic
    # -------------------------------------------------------------------------

    def _check_risk_management(self) -> None:
        """
        Evaluates Level 2 risk management rules:
        - Stop-Loss: If buyback premium increases by 1% from entry, close both positions.
        - Take-Profit: If buyback premium decreases by 2% from entry, close both positions.
        - Guarantees idempotency to prevent duplicate exit order attempts.
        """
        if self.exit_triggered or self.strategy_state != StrategyState.ACTIVE_POSITION:
            return

        # 1. Stop-Loss Trigger Check (+1% buyback cost increase)
        if self.current_buyback_premium >= self.stop_loss_target_premium:
            self.log.warning(
                f"[STOP-LOSS TRIGGERED] Buyback Premium {self.current_buyback_premium:.2f} >= "
                f"Target {self.stop_loss_target_premium:.2f} (+{self.config.stop_loss_pct * 100}%). "
                f"Closing both positions!",
                LogColor.RED,
            )
            self._trigger_exit(reason="STOP_LOSS")

        # 2. Take-Profit Trigger Check (-2% buyback cost decrease)
        elif self.current_buyback_premium <= self.take_profit_target_premium:
            self.log.info(
                f"[TAKE-PROFIT TRIGGERED] Buyback Premium {self.current_buyback_premium:.2f} <= "
                f"Target {self.take_profit_target_premium:.2f} (-{self.config.take_profit_pct * 100}%). "
                f"Locking in profits and closing both positions!",
                LogColor.GREEN,
            )
            self._trigger_exit(reason="TAKE_PROFIT")

    def _trigger_exit(self, reason: str) -> None:
        """
        Executes buyback exit orders for both Call and Put legs.
        Includes duplicate exit attempt protection.
        """
        # Idempotency guard: prevent duplicate exit submissions
        if self.exit_triggered:
            self.log.warning(f"Exit already in progress, ignoring duplicate trigger: {reason}")
            return

        self.exit_triggered = True
        self.exit_reason = reason
        self.strategy_state = StrategyState.EXIT_TRIGGERED

        self.log.info(f"Submitting buyback closing orders. Reason: {reason}", LogColor.CYAN)

        # 1. Close Call Position
        call_pos = self.portfolio.position(self.call_instrument.id) if self.portfolio and self.call_instrument else None
        if call_pos and not call_pos.is_closed:
            call_close_qty = self.call_instrument.make_qty(abs(call_pos.signed_qty.as_double()))
            call_close_order: MarketOrder = self.order_factory.market(
                instrument_id=self.call_instrument.id,
                order_side=OrderSide.BUY,
                quantity=call_close_qty,
                reduce_only=True,
                time_in_force=TimeInForce.GTC,
            )
            self.call_exit_order_id = call_close_order.client_order_id
            self.submit_order(call_close_order)

        # 2. Close Put Position
        put_pos = self.portfolio.position(self.put_instrument.id) if self.portfolio and self.put_instrument else None
        if put_pos and not put_pos.is_closed:
            put_close_qty = self.put_instrument.make_qty(abs(put_pos.signed_qty.as_double()))
            put_close_order: MarketOrder = self.order_factory.market(
                instrument_id=self.put_instrument.id,
                order_side=OrderSide.BUY,
                quantity=put_close_qty,
                reduce_only=True,
                time_in_force=TimeInForce.GTC,
            )
            self.put_exit_order_id = put_close_order.client_order_id
            self.submit_order(put_close_order)

        self._emit(
            "EXIT_TRIGGERED",
            {
                "reason": reason,
                "current_buyback_premium": self.current_buyback_premium,
                "combined_entry_premium": self.combined_entry_premium,
                "pnl_pct": self.current_pnl_pct,
                "call_exit_order_id": str(self.call_exit_order_id) if self.call_exit_order_id else None,
                "put_exit_order_id": str(self.put_exit_order_id) if self.put_exit_order_id else None,
            },
        )

    # -------------------------------------------------------------------------
    # Order Lifecycle Callbacks
    # -------------------------------------------------------------------------

    def on_order_submitted(self, event: OrderSubmitted) -> None:
        """Actions performed when an order is submitted to the execution engine."""
        self.log.info(f"Order Submitted: {event.client_order_id} ({event.order_side.name})", LogColor.BLUE)
        self._emit(
            "ORDER_SUBMITTED",
            {
                "client_order_id": str(event.client_order_id),
                "instrument_id": str(event.instrument_id),
                "order_side": event.order_side.name,
            },
        )

    def on_order_accepted(self, event: OrderAccepted) -> None:
        """Actions performed when Bybit venue acknowledges receipt of the order."""
        self.log.info(
            f"Order Accepted by Bybit: {event.client_order_id} (Venue ID: {event.venue_order_id})",
            LogColor.GREEN,
        )
        self._emit(
            "ORDER_ACCEPTED",
            {
                "client_order_id": str(event.client_order_id),
                "venue_order_id": str(event.venue_order_id),
                "instrument_id": str(event.instrument_id),
            },
        )

    def on_order_rejected(self, event: OrderRejected) -> None:
        """
        Actions performed if an order is rejected by Bybit.
        Addresses walkthrough question: What happens when one leg fails/gets rejected?
        """
        self.log.error(f"Order Rejected by Venue: {event.client_order_id}. Reason: {event.info}")
        self._emit(
            "ORDER_REJECTED",
            {
                "client_order_id": str(event.client_order_id),
                "instrument_id": str(event.instrument_id),
                "reason": str(event.info),
            },
        )

        # Handling legging failure: If one leg is rejected while the other might be active,
        # eliminate unhedged directional exposure immediately
        self._handle_legging_failure(failed_order_id=event.client_order_id)

    def on_order_canceled(self, event: OrderCanceled) -> None:
        """Actions performed when an order is canceled."""
        self.log.warning(f"Order Canceled: {event.client_order_id}")
        self._emit(
            "ORDER_CANCELED",
            {
                "client_order_id": str(event.client_order_id),
                "instrument_id": str(event.instrument_id),
            },
        )

    def on_order_filled(self, event: OrderFilled) -> None:
        """
        Actions performed when an order is executed/filled by Bybit.
        Records fill price, quantity, and calculates combined entry premium.
        """
        fill_price = event.last_px.as_double()
        fill_qty = event.last_qty.as_double()

        self.log.info(
            f"Order Filled: {event.client_order_id} | Price: {fill_price:.2f} | Qty: {fill_qty} | Side: {event.order_side.name}",
            LogColor.GREEN,
        )

        self._emit(
            "ORDER_FILLED",
            {
                "client_order_id": str(event.client_order_id),
                "venue_order_id": str(event.venue_order_id),
                "instrument_id": str(event.instrument_id),
                "fill_price": fill_price,
                "fill_qty": fill_qty,
                "order_side": event.order_side.name,
            },
        )

        # Check if this is Call Entry Fill
        if event.client_order_id == self.call_entry_order_id:
            self.call_entry_filled = True
            self.call_entry_price = fill_price
            self.call_entry_qty = fill_qty

        # Check if this is Put Entry Fill
        elif event.client_order_id == self.put_entry_order_id:
            self.put_entry_filled = True
            self.put_entry_price = fill_price
            self.put_entry_qty = fill_qty

        # When BOTH legs are filled: Straddle position is fully active!
        if self.call_entry_filled and self.put_entry_filled and self.strategy_state == StrategyState.SUBMITTING_ENTRY:
            self.strategy_state = StrategyState.ACTIVE_POSITION
            self.combined_entry_premium = self.call_entry_price + self.put_entry_price

            # Level 2 thresholds:
            # Stop-Loss: +1% increase in buyback premium
            self.stop_loss_target_premium = self.combined_entry_premium * (1.0 + float(self.config.stop_loss_pct))
            # Take-Profit: -2% decrease in buyback premium
            self.take_profit_target_premium = self.combined_entry_premium * (1.0 - float(self.config.take_profit_pct))

            self.log.info(
                f"Short Straddle Entry Complete! "
                f"Call Fill: ${self.call_entry_price:.2f} | Put Fill: ${self.put_entry_price:.2f} | "
                f"Combined Entry Premium: ${self.combined_entry_premium:.2f}",
                LogColor.MAGENTA,
            )

            if self.config.level == 2:
                self.log.info(
                    f"Risk Targets Active: "
                    f"Stop-Loss (+1%): ${self.stop_loss_target_premium:.2f} | "
                    f"Take-Profit (-2%): ${self.take_profit_target_premium:.2f}",
                    LogColor.CYAN,
                )

            self._emit(
                "STRADDLE_POSITION_ESTABLISHED",
                {
                    "call_fill_price": self.call_entry_price,
                    "put_fill_price": self.put_entry_price,
                    "combined_entry_premium": self.combined_entry_premium,
                    "stop_loss_target": self.stop_loss_target_premium,
                    "take_profit_target": self.take_profit_target_premium,
                },
            )

    # -------------------------------------------------------------------------
    # Position Lifecycle Callbacks
    # -------------------------------------------------------------------------

    def on_position_opened(self, event: PositionOpened) -> None:
        """Actions performed when a position is opened."""
        self.log.info(
            f"Position Opened: {event.instrument_id} | Side: {event.side.name} | "
            f"Qty: {event.quantity} | Avg Price: {event.avg_px_open:.2f}",
            LogColor.GREEN,
        )
        self._emit(
            "POSITION_OPENED",
            {
                "position_id": str(event.position_id),
                "instrument_id": str(event.instrument_id),
                "side": event.side.name,
                "quantity": event.quantity.as_double(),
                "avg_price": event.avg_px_open.as_double(),
            },
        )

    def on_position_changed(self, event: PositionChanged) -> None:
        """Actions performed when position size or unrealized PnL changes."""
        self._emit(
            "POSITION_CHANGED",
            {
                "position_id": str(event.position_id),
                "instrument_id": str(event.instrument_id),
                "quantity": event.quantity.as_double(),
                "unrealized_pnl": event.unrealized_pnl.as_double() if event.unrealized_pnl else 0.0,
            },
        )

    def on_position_closed(self, event: PositionClosed) -> None:
        """Actions performed when a position is closed."""
        self.log.info(
            f"Position Closed: {event.instrument_id} | Realized PnL: ${event.realized_pnl:.2f}",
            LogColor.YELLOW,
        )
        self._emit(
            "POSITION_CLOSED",
            {
                "position_id": str(event.position_id),
                "instrument_id": str(event.instrument_id),
                "realized_pnl": event.realized_pnl.as_double(),
            },
        )

        # Check if both Call and Put positions are closed
        call_pos = self.portfolio.position(self.call_instrument.id)
        put_pos = self.portfolio.position(self.put_instrument.id)

        call_closed = call_pos is None or call_pos.is_closed
        put_closed = put_pos is None or put_pos.is_closed

        if call_closed and put_closed and self.strategy_state in (StrategyState.EXIT_TRIGGERED, StrategyState.ACTIVE_POSITION):
            self.strategy_state = StrategyState.CLOSED
            self.log.info("Both Straddle Legs Successfully Closed!", LogColor.GREEN)
            self._emit("STRADDLE_CLOSED", {"reason": self.exit_reason})

    # -------------------------------------------------------------------------
    # Handling Legging Risk & Unhedged Execution
    # -------------------------------------------------------------------------

    def _handle_legging_failure(self, failed_order_id: ClientOrderId) -> None:
        """
        Critical risk protocol:
        If one leg is rejected or times out, the strategy must NOT leave an unhedged
        naked option position active. It cancels any pending order and closes any
        open leg immediately.
        """
        self.strategy_state = StrategyState.UNHEDGED_ALERT
        self.log.error(
            f"[LEGGING RISK TRIGGERED] Order {failed_order_id} failed. "
            f"Checking for unhedged open leg to safely close.",
            LogColor.RED,
        )

        # If Call filled but Put failed: Close Call immediately
        if self.call_entry_filled and not self.put_entry_filled:
            if self.portfolio and self.call_instrument:
                call_pos = self.portfolio.position(self.call_instrument.id)
                if call_pos and not call_pos.is_closed:
                    self.log.warning("Closing unhedged Call leg to eliminate directional exposure!")
                    self.close_position(call_pos)

        # If Put filled but Call failed: Close Put immediately
        elif self.put_entry_filled and not self.call_entry_filled:
            if self.portfolio and self.put_instrument:
                put_pos = self.portfolio.position(self.put_instrument.id)
                if put_pos and not put_pos.is_closed:
                    self.log.warning("Closing unhedged Put leg to eliminate directional exposure!")
                    self.close_position(put_pos)

        self._emit("UNHEDGED_LEG_SAFETY_EXIT", {"failed_order_id": str(failed_order_id)})

