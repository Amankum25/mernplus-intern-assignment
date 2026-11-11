"""
BTC Options Short Straddle Strategy for NautilusTrader.

===============================================================================
INTERVIEWER OVERVIEW & ARCHITECTURAL GUIDE:
===============================================================================
1. What is this strategy?
   - A Short Straddle involves selling an At-The-Money (ATM) Call option AND an
     At-The-Money (ATM) Put option with the exact same strike price and expiration date.
   - We collect premiums on both options upfront.
   - The strategy is delta-neutral at entry (Call Delta ~ +0.50, Put Delta ~ -0.50, Net Delta ~ 0.00).
   - Profit occurs if the underlying BTC price stays within a narrow trading range around
     the strike, allowing the options to lose value (theta decay).

2. What is Level 1 vs Level 2?
   - Level 1 (Naked Straddle): Sells the Call and Put and holds the position.
     No automated stop-loss or take-profit. Manual exit only.
   - Level 2 (Automated Risk Management): Continuously monitors live market quotes and
     automatically triggers closing buyback orders if:
       a) Stop-Loss (+1%): Combined buyback premium increases by >= 1% above the entry premium.
       b) Take-Profit (-2%): Combined buyback premium decreases by >= 2% below the entry premium.

3. How does Valuation work?
   - When SHORT options, to close the position we must BUY them back.
   - Buyers in any financial market buy at the ASK price (the lowest price sellers are offering).
   - Therefore:
       Combined Buyback Premium = Call Ask Price + Put Ask Price
       PnL ($) = (Combined Entry Premium - Combined Buyback Premium) * Trade Size
       PnL (%) = ((Combined Entry Premium - Combined Buyback Premium) / Combined Entry Premium) * 100

4. Key Safety Features:
   - Legging Risk Protection: If one leg fills but the other fails/rejects, the system
     immediately unwinds the open leg to prevent being left with unhedged directional exposure.
   - Idempotency Guard (`self.exit_triggered`): Ensures that exit orders are only sent ONCE,
     preventing duplicate orders when hundreds of quote ticks arrive each second.
   - `reduce_only=True`: Ensures closing orders can ONLY reduce an open position, never open
     an accidental reverse position.
===============================================================================
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


# =============================================================================
# STRATEGY FINITE STATE MACHINE (FSM)
# =============================================================================
class StrategyState:
    """
    Finite State Machine (FSM) states for the straddle position lifecycle.

    State Transition Flow:
      INITIALIZING -> WAITING_FOR_DATA -> SUBMITTING_ENTRY -> ACTIVE_POSITION -> EXIT_TRIGGERED -> CLOSED
                                                           \\
                                                            -> UNHEDGED_ALERT (if a leg fails)
    """
    INITIALIZING = "INITIALIZING"          # Resolving instruments from NautilusTrader's cache
    WAITING_FOR_DATA = "WAITING_FOR_DATA"  # Subscribed to market feeds, waiting for first quotes
    SUBMITTING_ENTRY = "SUBMITTING_ENTRY"  # Dispatched simultaneous SELL orders to Bybit
    ACTIVE_POSITION = "ACTIVE_POSITION"    # Both legs filled; live valuation and SL/TP active
    EXIT_TRIGGERED = "EXIT_TRIGGERED"      # SL or TP threshold breached; closing BUY orders dispatched
    CLOSED = "CLOSED"                      # Both legs successfully bought back; strategy finished
    UNHEDGED_ALERT = "UNHEDGED_ALERT"      # Legging risk: one leg failed, emergency unwind active


# =============================================================================
# MAIN STRATEGY IMPLEMENTATION
# =============================================================================
class BTCShortStraddleStrategy(Strategy):
    """
    NautilusTrader Strategy implementation for the BTC ATM Short Straddle.
    Inherits from `nautilus_trader.trading.strategy.Strategy`.

    Provides:
    - Lifecycle hooks: `on_start()`, `on_stop()`
    - Order submission: simultaneous Call & Put SELL market orders
    - Market quote processing: `on_quote_tick()` calculates buyback premium on every tick
    - Risk management: 1% Stop-Loss / 2% Take-Profit with idempotency guard
    - Event callbacks: `on_order_submitted`, `on_order_accepted`, `on_order_filled`, `on_position_closed`
    - Legging failure protocol: immediate unwind if an unhedged leg occurs
    """

    def __init__(self, config: BTCShortStraddleConfig) -> None:
        """
        Initialize strategy state, identifiers, and telemetry buffers.
        """
        super().__init__(config)

        # ---------------------------------------------------------------------
        # 1. Instrument Handles (resolved from NautilusTrader cache on start)
        # ---------------------------------------------------------------------
        self.call_instrument: Optional[Instrument] = None
        self.put_instrument: Optional[Instrument] = None

        # ---------------------------------------------------------------------
        # 2. State Machine & Idempotency Controls
        # ---------------------------------------------------------------------
        self.strategy_state: str = StrategyState.INITIALIZING
        # Idempotency guard: once set to True, prevents any second exit order
        self.exit_triggered: bool = False
        self.exit_reason: Optional[str] = None

        # ---------------------------------------------------------------------
        # 3. Order Identifiers (Client Order IDs generated client-side)
        # ---------------------------------------------------------------------
        self.call_entry_order_id: Optional[ClientOrderId] = None
        self.put_entry_order_id: Optional[ClientOrderId] = None
        self.call_exit_order_id: Optional[ClientOrderId] = None
        self.put_exit_order_id: Optional[ClientOrderId] = None

        # ---------------------------------------------------------------------
        # 4. Entry Fill Tracking & Baseline Premium
        # ---------------------------------------------------------------------
        self.call_entry_filled: bool = False
        self.put_entry_filled: bool = False
        self.call_entry_price: float = 0.0
        self.put_entry_price: float = 0.0
        self.call_entry_qty: float = 0.0
        self.put_entry_qty: float = 0.0
        # Combined entry premium = Call Fill Price + Put Fill Price
        # This is the total money collected per BTC of straddle sold
        self.combined_entry_premium: float = 0.0

        # ---------------------------------------------------------------------
        # 5. Live Market Pricing & Buyback Valuation
        # ---------------------------------------------------------------------
        # We track ask prices because buying back short options costs the ask price
        self.current_call_ask: float = 0.0
        self.current_put_ask: float = 0.0
        self.current_call_bid: float = 0.0
        self.current_put_bid: float = 0.0
        self.current_buyback_premium: float = 0.0
        self.current_pnl_pct: float = 0.0
        self.current_pnl_usd: float = 0.0

        # ---------------------------------------------------------------------
        # 6. Level 2 Risk Management Targets (calculated once both legs fill)
        # ---------------------------------------------------------------------
        self.stop_loss_target_premium: float = 0.0   # Entry * (1.0 + 0.01) -> +1% cost rise
        self.take_profit_target_premium: float = 0.0 # Entry * (1.0 - 0.02) -> -2% cost fall

        # ---------------------------------------------------------------------
        # 7. Timing & Telemetry
        # ---------------------------------------------------------------------
        self.entry_start_time: float = 0.0
        self.last_quote_time: float = 0.0

        # Event listener for dispatching telemetry updates to the UI / WebSocket
        self.event_listener: Optional[Callable[[Dict[str, Any]], None]] = None
        self.execution_logs: List[Dict[str, Any]] = []

    def set_event_listener(self, listener: Callable[[Dict[str, Any]], None]) -> None:
        """
        Register a callback function to receive all strategy lifecycle events.
        Used by the Execution Manager to stream real-time updates over WebSockets.
        """
        self.event_listener = listener

    def _emit(self, event_type: str, data: Dict[str, Any]) -> None:
        """
        Helper method to append an event to the local log and notify the listener.
        """
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

    # =========================================================================
    # STRATEGY LIFECYCLE (on_start, on_stop)
    # =========================================================================

    def on_start(self) -> None:
        """
        NautilusTrader lifecycle hook called when the strategy engine starts.

        Execution steps:
        1. Resolve Call and Put instrument definitions from NautilusTrader cache.
        2. Subscribe to market data feeds (QuoteTick and TradeTick) for both legs.
        3. Transition state to WAITING_FOR_DATA.
        4. Trigger the straddle entry via `enter_straddle()`.
        """
        self.log.info(
            f"Starting BTC Short Straddle Strategy [Level {self.config.level}]",
            LogColor.BLUE,
        )

        # Step 1: Look up instruments in cache (loaded by ExecutionManager before start)
        self.call_instrument = self.cache.instrument(self.config.call_instrument_id)
        self.put_instrument = self.cache.instrument(self.config.put_instrument_id)

        # Safety check: if instruments are missing, stop immediately
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

        # Step 2: Subscribe to live quote ticks (bid/ask) and trade ticks for both instruments
        self.subscribe_quote_ticks(self.call_instrument.id)
        self.subscribe_quote_ticks(self.put_instrument.id)
        self.subscribe_trade_ticks(self.call_instrument.id)
        self.subscribe_trade_ticks(self.put_instrument.id)

        # Step 3: Transition state to WAITING_FOR_DATA and emit start event
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

        # Step 4: Dispatch the entry orders
        self.enter_straddle()

    def on_stop(self) -> None:
        """
        NautilusTrader lifecycle hook called when the strategy engine is stopped.
        Cleans up resources and emits final state.
        """
        self.log.info("Stopping BTC Short Straddle Strategy", LogColor.YELLOW)
        self._emit("STRATEGY_STOPPED", {"final_state": self.strategy_state})

    # =========================================================================
    # STRADDLE ENTRY (Simultaneous Order Placement)
    # =========================================================================

    def enter_straddle(self) -> None:
        """
        Submits simultaneous SELL market orders for both the Call and Put legs.

        Why simultaneous?
        - In options trading, entering a straddle requires selling both legs at once.
        - Submitting them back-to-back minimizes the time gap (legging risk)
          where market moves could cause asymmetric execution prices.
        """
        # Guard: only submit entry if currently initializing or waiting for data
        if self.strategy_state not in (StrategyState.WAITING_FOR_DATA, StrategyState.INITIALIZING):
            return

        self.strategy_state = StrategyState.SUBMITTING_ENTRY
        self.entry_start_time = time.time()
        order_qty = self.call_instrument.make_qty(self.config.trade_size)

        self.log.info(
            f"Submitting Short Straddle Entry: Sell {order_qty} Call & {order_qty} Put",
            LogColor.CYAN,
        )

        # 1. Create Call Leg SELL market order
        call_order: MarketOrder = self.order_factory.market(
            instrument_id=self.call_instrument.id,
            order_side=OrderSide.SELL,
            quantity=order_qty,
            time_in_force=TimeInForce.GTC,
        )
        self.call_entry_order_id = call_order.client_order_id

        # 2. Create Put Leg SELL market order (identical size and time-in-force)
        put_order: MarketOrder = self.order_factory.market(
            instrument_id=self.put_instrument.id,
            order_side=OrderSide.SELL,
            quantity=order_qty,
            time_in_force=TimeInForce.GTC,
        )
        self.put_entry_order_id = put_order.client_order_id

        # 3. Submit both orders to the NautilusTrader execution engine
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

    # =========================================================================
    # REAL-TIME MARKET DATA & VALUATION
    # =========================================================================

    def on_quote_tick(self, tick: QuoteTick) -> None:
        """
        NautilusTrader callback fired on every incoming bid/ask quote tick from Bybit.

        Valuation Logic:
        1. Updates current Call or Put ask/bid prices from the incoming tick.
        2. If both legs are filled (`ACTIVE_POSITION`), recalculates the combined buyback premium:
             Combined Buyback = Call Ask Price + Put Ask Price
           (We use ask prices because exiting a short option requires BUYING at the ask).
        3. Calculates PnL:
             PnL % = (Entry Premium - Buyback Premium) / Entry Premium * 100
             PnL $ = (Entry Premium - Buyback Premium) * Trade Size
        4. If Level 2 is enabled, checks automated Stop-Loss (+1%) and Take-Profit (-2%).
        """
        self.last_quote_time = time.time()

        # Update Call quotes if this tick belongs to the Call contract
        if tick.instrument_id == self.call_instrument.id:
            self.current_call_ask = tick.ask_price.as_double()
            self.current_call_bid = tick.bid_price.as_double()

        # Update Put quotes if this tick belongs to the Put contract
        elif tick.instrument_id == self.put_instrument.id:
            self.current_put_ask = tick.ask_price.as_double()
            self.current_put_bid = tick.bid_price.as_double()

        # Calculate valuation only when both legs are filled and quotes are valid (> 0)
        if self.strategy_state == StrategyState.ACTIVE_POSITION and self.current_call_ask > 0 and self.current_put_ask > 0:
            # Current combined cost to close both positions right now
            self.current_buyback_premium = self.current_call_ask + self.current_put_ask

            if self.combined_entry_premium > 0:
                # Profit occurs when buyback cost is LESS than entry premium
                premium_diff = self.combined_entry_premium - self.current_buyback_premium
                self.current_pnl_pct = (premium_diff / self.combined_entry_premium) * 100.0
                self.current_pnl_usd = premium_diff * float(self.config.trade_size)

                # Broadcast live valuation update to dashboard
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

                # Evaluate automated risk thresholds if Level 2 is enabled
                if self.config.level == 2:
                    self._check_risk_management()

    def on_trade_tick(self, tick: TradeTick) -> None:
        """
        NautilusTrader callback fired when an executed trade occurs in the market.
        (Logged for telemetry; risk valuation relies on QuoteTicks).
        """
        pass

    # =========================================================================
    # LEVEL 2 AUTOMATED RISK MANAGEMENT (1% SL / 2% TP)
    # =========================================================================

    def _check_risk_management(self) -> None:
        """
        Evaluates Level 2 risk management rules on every quote update:

        1. Stop-Loss Trigger Check (+1% buyback cost increase):
           - Formula: V_buyback >= V_entry * 1.01
           - Meaning: Options gained value against us; closing now limits loss to 1%.

        2. Take-Profit Trigger Check (-2% buyback cost decrease):
           - Formula: V_buyback <= V_entry * 0.98
           - Meaning: Options decayed by 2%; closing now locks in the 2% profit.

        - Idempotency guard: If an exit is already underway (`self.exit_triggered == True`),
          no secondary exit orders can be dispatched.
        """
        # Guard: do nothing if exit is already triggered or position is not active
        if self.exit_triggered or self.strategy_state != StrategyState.ACTIVE_POSITION:
            return

        # 1. Stop-Loss check
        if self.current_buyback_premium >= self.stop_loss_target_premium:
            self.log.warning(
                f"[STOP-LOSS TRIGGERED] Buyback Premium {self.current_buyback_premium:.2f} >= "
                f"Target {self.stop_loss_target_premium:.2f} (+{self.config.stop_loss_pct * 100}%). "
                f"Closing both positions!",
                LogColor.RED,
            )
            self._trigger_exit(reason="STOP_LOSS")

        # 2. Take-Profit check
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
        Dispatches BUY market orders to buy back and close both short positions.

        Key Safety Details:
        - `self.exit_triggered = True`: Sets the idempotency guard immediately.
        - `reduce_only=True`: Ensures the exchange strictly closes the existing short
          positions without any possibility of opening an accidental long position.
        """
        # Double-check idempotency guard
        if self.exit_triggered:
            self.log.warning(f"Exit already in progress, ignoring duplicate trigger: {reason}")
            return

        # Set idempotency guard and transition state
        self.exit_triggered = True
        self.exit_reason = reason
        self.strategy_state = StrategyState.EXIT_TRIGGERED

        self.log.info(f"Submitting buyback closing orders. Reason: {reason}", LogColor.CYAN)

        # 1. Close Call Position (send BUY market order with reduce_only=True)
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

        # 2. Close Put Position (send BUY market order with reduce_only=True)
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

    # =========================================================================
    # ORDER LIFECYCLE CALLBACKS (NautilusTrader Event Handlers)
    # =========================================================================

    def on_order_submitted(self, event: OrderSubmitted) -> None:
        """
        Called when an order is submitted to NautilusTrader's execution engine.
        Confirms local dispatch before venue transmission.
        """
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
        """
        Called when Bybit venue acknowledges receipt of the order.
        The venue assigns its own `venue_order_id` which we track.
        """
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
        Called if Bybit rejects the order (e.g., insufficient margin, invalid size).
        Triggers the legging risk handler to ensure no single leg remains open naked.
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

        # Trigger emergency legging failure recovery
        self._handle_legging_failure(failed_order_id=event.client_order_id)

    def on_order_canceled(self, event: OrderCanceled) -> None:
        """
        Called when an order is canceled (by user, timeout, or venue).
        """
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
        Called when an order is executed/filled by Bybit.

        Lifecycle Logic:
        1. Records fill price and fill quantity for the Call or Put leg.
        2. Once BOTH Call and Put entry legs are filled:
           - Transitions state from SUBMITTING_ENTRY to ACTIVE_POSITION.
           - Calculates Combined Entry Premium = Call Fill Price + Put Fill Price.
           - Sets Stop-Loss target: Entry * (1.0 + 0.01)
           - Sets Take-Profit target: Entry * (1.0 - 0.02)
           - Emits `STRADDLE_POSITION_ESTABLISHED` event.
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

        # Check if this is the Call Entry Order fill
        if event.client_order_id == self.call_entry_order_id:
            self.call_entry_filled = True
            self.call_entry_price = fill_price
            self.call_entry_qty = fill_qty

        # Check if this is the Put Entry Order fill
        elif event.client_order_id == self.put_entry_order_id:
            self.put_entry_filled = True
            self.put_entry_price = fill_price
            self.put_entry_qty = fill_qty

        # Once BOTH legs are filled, activate the straddle and lock in baseline targets
        if self.call_entry_filled and self.put_entry_filled and self.strategy_state == StrategyState.SUBMITTING_ENTRY:
            self.strategy_state = StrategyState.ACTIVE_POSITION
            self.combined_entry_premium = self.call_entry_price + self.put_entry_price

            # Calculate Level 2 Stop-Loss (+1%) and Take-Profit (-2%) price thresholds
            self.stop_loss_target_premium = self.combined_entry_premium * (1.0 + float(self.config.stop_loss_pct))
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

    # =========================================================================
    # POSITION LIFECYCLE CALLBACKS (NautilusTrader Portfolio Integration)
    # =========================================================================

    def on_position_opened(self, event: PositionOpened) -> None:
        """
        Called when NautilusTrader's Portfolio opens an option position.
        Tracks signed quantity and average entry price.
        """
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
        """
        Called when an open position's size or unrealized PnL updates.
        """
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
        """
        Called when a position is closed.
        Checks if both Call and Put positions are closed; if so, marks strategy as CLOSED.
        """
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

        # Check if both positions are now flat in the portfolio
        call_pos = self.portfolio.position(self.call_instrument.id)
        put_pos = self.portfolio.position(self.put_instrument.id)

        call_closed = call_pos is None or call_pos.is_closed
        put_closed = put_pos is None or put_pos.is_closed

        if call_closed and put_closed and self.strategy_state in (StrategyState.EXIT_TRIGGERED, StrategyState.ACTIVE_POSITION):
            self.strategy_state = StrategyState.CLOSED
            self.log.info("Both Straddle Legs Successfully Closed!", LogColor.GREEN)
            self._emit("STRADDLE_CLOSED", {"reason": self.exit_reason})

    # =========================================================================
    # LEGGING RISK HANDLING (Emergency Protocol)
    # =========================================================================

    def _handle_legging_failure(self, failed_order_id: ClientOrderId) -> None:
        """
        CRITICAL SAFETY PROTOCOL: Handles Legging Risk.

        What is Legging Risk?
        - A straddle is designed to be delta-neutral by holding BOTH a Call and a Put.
        - If the Call fills, but the Put gets rejected or fails to fill, we are left
          holding a naked short Call — which has unlimited directional loss potential
          if BTC pumps!
        - Similarly, holding a naked short Put leaves us vulnerable if BTC crashes.

        Solution:
        - Detect if one leg filled and the other failed.
        - Immediately send a market order to close the filled leg.
        - Returns portfolio to flat (cash), eliminating directional risk.
        """
        self.strategy_state = StrategyState.UNHEDGED_ALERT
        self.log.error(
            f"[LEGGING RISK TRIGGERED] Order {failed_order_id} failed. "
            f"Checking for unhedged open leg to safely close.",
            LogColor.RED,
        )

        # Case 1: Call leg filled, but Put leg failed -> Close the Call immediately
        if self.call_entry_filled and not self.put_entry_filled:
            if self.portfolio and self.call_instrument:
                call_pos = self.portfolio.position(self.call_instrument.id)
                if call_pos and not call_pos.is_closed:
                    self.log.warning("Closing unhedged Call leg to eliminate directional exposure!")
                    self.close_position(call_pos)

        # Case 2: Put leg filled, but Call leg failed -> Close the Put immediately
        elif self.put_entry_filled and not self.call_entry_filled:
            if self.portfolio and self.put_instrument:
                put_pos = self.portfolio.position(self.put_instrument.id)
                if put_pos and not put_pos.is_closed:
                    self.log.warning("Closing unhedged Put leg to eliminate directional exposure!")
                    self.close_position(put_pos)

        self._emit("UNHEDGED_LEG_SAFETY_EXIT", {"failed_order_id": str(failed_order_id)})
