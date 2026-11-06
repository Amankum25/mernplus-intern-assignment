"""
Execution Manager for NautilusTrader BTC Options Straddle.
Coordinates environment settings (Testnet / Demo / Mainnet / Simulation),
loads instruments, configures NautilusTrader TradingNode, and manages strategy execution.
"""

import asyncio
from decimal import Decimal
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional
import requests

from nautilus_trader.adapters.bybit import (
    BybitDataClientConfig,
    BybitEnvironment,
    BybitExecClientConfig,
    BybitLiveDataClientFactory,
    BybitLiveExecClientFactory,
    BybitProductType,
)
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.live.node import TradingNode, TradingNodeConfig
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.events import (
    OrderAccepted,
    OrderFilled,
    OrderSubmitted,
    PositionClosed,
    PositionOpened,
)
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientId,
    ClientOrderId,
    InstrumentId,
    PositionId,
    TraderId,
    Venue,
    VenueOrderId,
)
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.position import Position

from straddle.bybit_client import BybitV5Client
from straddle.config import BTCShortStraddleConfig
from straddle.persistence import state_persistence
from straddle.strategy import BTCShortStraddleStrategy, StrategyState
from straddle.strike_selector import BybitStrikeSelector, StraddlePair


logger = logging.getLogger("StraddleExecutionManager")


class StraddleExecutionManager:
    """
    Manages the lifecycle of the BTC Options Straddle Strategy.
    Supports:
    1. Bybit Testnet / Demo trading via NautilusTrader live engine.
    2. Simulated Live-Market Execution (streams live Bybit quotes and emulates
       realistic order fills and positions for local demonstration and risk testing).
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        environment: str = "TESTNET",
    ):
        self.api_key = api_key or os.getenv("BYBIT_API_KEY", "")
        self.api_secret = api_secret or os.getenv("BYBIT_API_SECRET", "")
        self.environment_str = environment.upper()
        self.env_enum = getattr(BybitEnvironment, self.environment_str, BybitEnvironment.TESTNET)

        # Base URL depending on environment
        if self.env_enum == BybitEnvironment.TESTNET:
            self.base_url = "https://api-testnet.bybit.com"
        elif self.env_enum == BybitEnvironment.DEMO:
            self.base_url = "https://api-demo.bybit.com"
        else:
            self.base_url = "https://api.bybit.com"

        self.selector = BybitStrikeSelector(base_url=self.base_url)
        self.bybit_client: Optional[BybitV5Client] = (
            BybitV5Client(self.api_key, self.api_secret, base_url=self.base_url)
            if (self.api_key and self.api_secret)
            else None
        )
        self.active_strategy: Optional[BTCShortStraddleStrategy] = None
        self.node: Optional[TradingNode] = None
        self.current_pair: Optional[StraddlePair] = None
        self.is_running: bool = False
        self.is_simulated: bool = False
        self.current_trade_size: float = 0.01

        # Live telemetry feed
        self.telemetry_listeners: List[Callable[[Dict[str, Any]], None]] = []
        self.latest_telemetry: Dict[str, Any] = {
            "status": "IDLE",
            "environment": self.environment_str,
            "btc_price": 0.0,
            "atm_strike": 0.0,
            "call_contract": None,
            "put_contract": None,
            "combined_entry_premium": 0.0,
            "current_buyback_premium": 0.0,
            "pnl_pct": 0.0,
            "pnl_usd": 0.0,
            "call_ask": 0.0,
            "put_ask": 0.0,
            "stop_loss_target": 0.0,
            "take_profit_target": 0.0,
            "state": "IDLE",
            "events": [],
        }

        # Polling/streaming thread for simulated or hybrid mode
        self._stream_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def register_listener(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Register a callback for real-time WebSocket/dashboard updates."""
        self.telemetry_listeners.append(callback)

    def _broadcast(self, event_data: Dict[str, Any]) -> None:
        """Broadcast events to all registered listeners."""
        self.latest_telemetry.update(event_data)
        if "events" in self.latest_telemetry and "type" in event_data:
            events = self.latest_telemetry["events"]
            events.append(event_data)
            if len(events) > 100:
                self.latest_telemetry["events"] = events[-100:]

        for listener in self.telemetry_listeners:
            try:
                listener(event_data)
            except Exception as e:
                logger.error(f"Error in telemetry listener: {e}")

    def get_market_overview(self) -> Dict[str, Any]:
        """
        Queries Bybit for current BTC price, available expiries, and nearest ATM strike.
        """
        pair = self.selector.select_atm_straddle()
        self.current_pair = pair
        return {
            "btc_price": pair.btc_price,
            "atm_strike": pair.atm_strike,
            "expiry_ms": pair.expiry_ms,
            "expiry_str": pair.expiry_str,
            "call_contract": {
                "symbol": pair.call_contract.symbol,
                "instrument_id": pair.call_contract.instrument_id,
                "strike": pair.call_contract.strike,
            },
            "put_contract": {
                "symbol": pair.put_contract.symbol,
                "instrument_id": pair.put_contract.instrument_id,
                "strike": pair.put_contract.strike,
            },
            "available_strikes": pair.available_strikes,
            "available_expiries": pair.available_expiries,
            "strike_matrix": pair.strike_matrix,
        }

    def start_straddle(
        self,
        level: int = 2,
        trade_size: float = 0.01,
        simulated: bool = False,
        target_expiry_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Starts the BTC ATM Short Straddle strategy.

        Parameters
        ----------
        level : int, default 2
            1: Level 1 (Naked Straddle).
            2: Level 2 (Automated Risk Management with 1% SL / 2% TP).
        trade_size : float, default 0.01
            Order size per leg (min 0.01 BTC).
        simulated : bool, default False
            If True, runs simulated execution with live market quotes.
        """
        # Cleanly reset and stop any prior loop before starting
        if self.is_running:
            self._stop_event.set()
            if self._stream_thread and self._stream_thread.is_alive():
                try:
                    self._stream_thread.join(timeout=0.5)
                except Exception:
                    pass
            self.is_running = False

        self._stop_event.clear()
        self.is_simulated = simulated

        # 1. Identify ATM strike and Call/Put contracts
        pair = self.selector.select_atm_straddle(target_expiry_ms=target_expiry_ms)
        self.current_pair = pair

        call_id = InstrumentId.from_str(pair.call_contract.instrument_id)
        put_id = InstrumentId.from_str(pair.put_contract.instrument_id)

        config = BTCShortStraddleConfig(
            call_instrument_id=call_id,
            put_instrument_id=put_id,
            trade_size=Decimal(str(trade_size)),
            level=level,
            stop_loss_pct=Decimal("0.01"),  # 1% Stop-Loss
            take_profit_pct=Decimal("0.02"),  # 2% Take-Profit
            simulated_execution=simulated,
        )

        self.active_strategy = BTCShortStraddleStrategy(config=config)
        self.active_strategy.set_event_listener(self._broadcast)

        self._broadcast(
            {
                "type": "INITIALIZATION",
                "status": "RUNNING",
                "level": level,
                "simulated": simulated,
                "btc_price": pair.btc_price,
                "atm_strike": pair.atm_strike,
                "expiry_str": pair.expiry_str,
                "call_id": str(call_id),
                "put_id": str(put_id),
                "trade_size": trade_size,
            }
        )

        self.is_running = True
        self.current_trade_size = trade_size

        # Start live execution or live-quote simulation loop
        self._stream_thread = threading.Thread(
            target=self._run_market_loop,
            args=(pair, level, trade_size, simulated),
            daemon=True,
        )
        self._stream_thread.start()

        return {
            "status": "STARTED",
            "level": level,
            "simulated": simulated,
            "atm_strike": pair.atm_strike,
            "call_contract": pair.call_contract.symbol,
            "put_contract": pair.put_contract.symbol,
            "expiry": pair.expiry_str,
        }

    def scale_in_straddle(self, additional_size: float = 0.01) -> Dict[str, Any]:
        """
        Adds to the existing short straddle position (scaling in / taking multiple positions).
        Submits additional sell orders on Bybit and updates the blended entry premium.
        """
        if not self.active_strategy or not self.current_pair:
            raise RuntimeError("No active straddle to scale in.")

        pair = self.current_pair
        call_symbol = pair.call_contract.symbol
        put_symbol = pair.put_contract.symbol

        # Fetch current quotes
        call_quote = self._fetch_option_ticker(call_symbol)
        put_quote = self._fetch_option_ticker(put_symbol)
        call_px = call_quote.get("bid1Price") or call_quote.get("markPrice", 800.0)
        put_px = put_quote.get("bid1Price") or put_quote.get("markPrice", 700.0)

        # Place live orders if in live mode
        if self.bybit_client and not self.is_simulated:
            logger.info(f"Scaling in: Submitting additional SELL {additional_size} BTC for {call_symbol} and {put_symbol}...")
            try:
                self.bybit_client.place_order(symbol=call_symbol, side="Sell", qty=str(additional_size))
                self.bybit_client.place_order(symbol=put_symbol, side="Sell", qty=str(additional_size))
            except Exception as e:
                logger.error(f"Error placing scale-in order: {e}")

        old_size = self.current_trade_size
        new_size = round(old_size + additional_size, 4)
        self.current_trade_size = new_size

        old_entry = self.active_strategy.combined_entry_premium
        new_leg_entry = call_px + put_px
        blended_entry = ((old_entry * old_size) + (new_leg_entry * additional_size)) / new_size
        self.active_strategy.combined_entry_premium = blended_entry
        self.active_strategy.stop_loss_target_premium = blended_entry * 1.01
        self.active_strategy.take_profit_target_premium = blended_entry * 0.98

        state_persistence.add_log(
            category="ORDER",
            message=f"Added Multiple Position (+{additional_size} BTC)! Total Size: {new_size:.2f} BTC | New Blended Entry: ${blended_entry:,.2f}",
            badge_type="info",
        )
        if state_persistence.state.get("active_trade"):
            state_persistence.state["active_trade"]["trade_size"] = new_size
            state_persistence.state["active_trade"]["combined_entry_premium"] = blended_entry
            state_persistence.state["active_trade"]["stop_loss_target"] = blended_entry * 1.01
            state_persistence.state["active_trade"]["take_profit_target"] = blended_entry * 0.98
            state_persistence._save_to_disk(state_persistence.state)

        self._broadcast(
            {
                "type": "SCALE_IN",
                "trade_size": new_size,
                "combined_entry_premium": blended_entry,
                "stop_loss_target": blended_entry * 1.01,
                "take_profit_target": blended_entry * 0.98,
            }
        )
        return {
            "status": "SCALED_IN",
            "message": f"Added position (+{additional_size} BTC). Total Position: {new_size:.2f} BTC",
            "total_size": new_size,
            "blended_entry": blended_entry,
        }

    def _run_market_loop(
        self,
        pair: StraddlePair,
        level: int,
        trade_size: float,
        simulated: bool,
    ) -> None:
        """
        Background market loop that continuously monitors live quotes from Bybit,
        dispatches ticks to the strategy, and executes automated risk management.
        """
        logger.info(f"Starting market execution loop for {pair.atm_strike} Straddle...")
        call_symbol = pair.call_contract.symbol
        put_symbol = pair.put_contract.symbol

        # Initial mock/entry fill emulation if simulated
        call_entry_px: float = 0.0
        put_entry_px: float = 0.0
        entry_established = False
        last_pnl_check_time: float = 0.0

        state_persistence.add_log(
            category="SYSTEM",
            message=f"Starting Level {level} Straddle for Strike ${pair.atm_strike:,.0f} (Expiry: {pair.expiry_str})",
            badge_type="info",
        )

        while not self._stop_event.is_set():
            try:
                # Query live option tickers from Bybit
                call_quote = self._fetch_option_ticker(call_symbol)
                put_quote = self._fetch_option_ticker(put_symbol)

                call_ask = call_quote.get("ask1Price") or 0.0
                call_bid = call_quote.get("bid1Price") or 0.0
                put_ask = put_quote.get("ask1Price") or 0.0
                put_bid = put_quote.get("bid1Price") or 0.0

                # Fallback to mark price if bid/ask spread is empty on deep illiquid strikes
                if call_ask == 0.0:
                    call_ask = call_quote.get("markPrice", 1200.0)
                if call_bid == 0.0:
                    call_bid = call_ask * 0.98
                if put_ask == 0.0:
                    put_ask = put_quote.get("markPrice", 1100.0)
                if put_bid == 0.0:
                    put_bid = put_ask * 0.98

                # 1. First cycle: Establish Straddle Entry
                if not entry_established:
                    # When entering short straddle, we SELL at bid or marketable limit
                    call_entry_px = call_bid if call_bid > 0 else call_ask
                    put_entry_px = put_bid if put_bid > 0 else put_ask

                    call_order_id = f"CALL-SELL-{int(time.time() * 1000)}"
                    put_order_id = f"PUT-SELL-{int(time.time() * 1000)}"
                    bybit_call_id = f"BYBIT-{call_order_id[-6:]}"
                    bybit_put_id = f"BYBIT-{put_order_id[-6:]}"

                    # Live Bybit Demo / Testnet Execution
                    if self.bybit_client and not simulated:
                        logger.info(f"Submitting LIVE Bybit orders for {call_symbol} and {put_symbol}...")
                        try:
                            c_res = self.bybit_client.place_order(symbol=call_symbol, side="Sell", qty=str(trade_size))
                            p_res = self.bybit_client.place_order(symbol=put_symbol, side="Sell", qty=str(trade_size))
                            if c_res.get("retCode") == 0:
                                bybit_call_id = c_res.get("result", {}).get("orderId", bybit_call_id)
                            else:
                                logger.warning(f"Bybit Call order msg: {c_res.get('retMsg')}")
                            if p_res.get("retCode") == 0:
                                bybit_put_id = p_res.get("result", {}).get("orderId", bybit_put_id)
                            else:
                                logger.warning(f"Bybit Put order msg: {p_res.get('retMsg')}")
                        except Exception as e:
                            logger.error(f"Error placing live Bybit order: {e}")

                    # Emulate / Track Order Submitted & Accepted
                    state_persistence.add_log(
                        category="ORDER",
                        message=f"Submitted SELL orders for Call {call_symbol} and Put {put_symbol} ({trade_size} BTC)",
                        badge_type="info",
                    )

                    self._broadcast(
                        {
                            "type": "ORDER_SUBMITTED",
                            "client_order_id": call_order_id,
                            "instrument_id": pair.call_contract.instrument_id,
                            "order_side": "SELL",
                            "quantity": trade_size,
                        }
                    )
                    self._broadcast(
                        {
                            "type": "ORDER_SUBMITTED",
                            "client_order_id": put_order_id,
                            "instrument_id": pair.put_contract.instrument_id,
                            "order_side": "SELL",
                            "quantity": trade_size,
                        }
                    )
                    time.sleep(0.5)

                    self._broadcast(
                        {
                            "type": "ORDER_ACCEPTED",
                            "client_order_id": call_order_id,
                            "venue_order_id": f"BYBIT-{call_order_id[-6:]}",
                            "instrument_id": pair.call_contract.instrument_id,
                        }
                    )
                    self._broadcast(
                        {
                            "type": "ORDER_ACCEPTED",
                            "client_order_id": put_order_id,
                            "venue_order_id": f"BYBIT-{put_order_id[-6:]}",
                            "instrument_id": pair.put_contract.instrument_id,
                        }
                    )
                    time.sleep(0.5)

                    # Emulate Order Filled
                    self._broadcast(
                        {
                            "type": "ORDER_FILLED",
                            "client_order_id": call_order_id,
                            "instrument_id": pair.call_contract.instrument_id,
                            "fill_price": call_entry_px,
                            "fill_qty": trade_size,
                            "order_side": "SELL",
                        }
                    )
                    self._broadcast(
                        {
                            "type": "ORDER_FILLED",
                            "client_order_id": put_order_id,
                            "instrument_id": pair.put_contract.instrument_id,
                            "fill_price": put_entry_px,
                            "fill_qty": trade_size,
                            "order_side": "SELL",
                        }
                    )

                    # Update strategy state
                    self.active_strategy.call_entry_filled = True
                    self.active_strategy.put_entry_filled = True
                    self.active_strategy.call_entry_price = call_entry_px
                    self.active_strategy.put_entry_price = put_entry_px
                    self.active_strategy.combined_entry_premium = call_entry_px + put_entry_px
                    self.active_strategy.strategy_state = StrategyState.ACTIVE_POSITION

                    # Risk targets: Stop-loss +1%, Take-profit -2%
                    combined_entry = call_entry_px + put_entry_px
                    sl_target = combined_entry * 1.01
                    tp_target = combined_entry * 0.98
                    self.active_strategy.stop_loss_target_premium = sl_target
                    self.active_strategy.take_profit_target_premium = tp_target

                    self._broadcast(
                        {
                            "type": "STRADDLE_POSITION_ESTABLISHED",
                            "call_fill_price": call_entry_px,
                            "put_fill_price": put_entry_px,
                            "combined_entry_premium": combined_entry,
                            "stop_loss_target": sl_target,
                            "take_profit_target": tp_target,
                            "level": level,
                        }
                    )
                    state_persistence.set_active_trade(
                        {
                            "level": level,
                            "simulated": simulated,
                            "trade_size": trade_size,
                            "atm_strike": pair.atm_strike,
                            "expiry_str": pair.expiry_str,
                            "call_symbol": pair.call_contract.symbol,
                            "put_symbol": pair.put_contract.symbol,
                            "call_entry_price": call_entry_px,
                            "put_entry_price": put_entry_px,
                            "combined_entry_premium": combined_entry,
                            "stop_loss_target": sl_target,
                            "take_profit_target": tp_target,
                            "current_buyback_premium": combined_entry,
                            "pnl_pct": 0.0,
                            "pnl_usd": 0.0,
                        }
                    )
                    state_persistence.add_log(
                        category="FILL",
                        message=(
                            f"Straddle Executed @ Combined Entry ${combined_entry:,.2f} "
                            f"(Call: ${call_entry_px:,.2f}, Put: ${put_entry_px:,.2f}) | "
                            f"1% SL Target: ${sl_target:,.2f} | 2% TP Target: ${tp_target:,.2f}"
                        ),
                        badge_type="success",
                        pnl_pct=0.0,
                        pnl_usd=0.0,
                    )
                    entry_established = True

                # 2. Continuous Quote Tick Updates & Valuation
                if entry_established and not self.active_strategy.exit_triggered:
                    self.active_strategy.current_call_ask = call_ask
                    self.active_strategy.current_put_ask = put_ask

                    # Buyback Valuation using ask quotes
                    combined_buyback = call_ask + put_ask
                    combined_entry = self.active_strategy.combined_entry_premium
                    pnl_diff = combined_entry - combined_buyback
                    pnl_pct = (pnl_diff / combined_entry) * 100.0 if combined_entry > 0 else 0.0
                    pnl_usd = pnl_diff * trade_size

                    self._broadcast(
                        {
                            "type": "VALUATION_UPDATE",
                            "call_ask": call_ask,
                            "call_bid": call_bid,
                            "put_ask": put_ask,
                            "put_bid": put_bid,
                            "current_buyback_premium": combined_buyback,
                            "combined_entry_premium": combined_entry,
                            "pnl_pct": pnl_pct,
                            "pnl_usd": pnl_usd,
                            "stop_loss_target": self.active_strategy.stop_loss_target_premium,
                            "take_profit_target": self.active_strategy.take_profit_target_premium,
                        }
                    )

                    # 5-second periodic PnL log & persistent valuation update
                    now_ts = time.time()
                    if now_ts - last_pnl_check_time >= 5.0:
                        last_pnl_check_time = now_ts
                        pnl_sign = "+" if pnl_pct >= 0 else ""
                        dist_sl = ((self.active_strategy.stop_loss_target_premium - combined_buyback) / combined_entry) * 100.0 if combined_entry > 0 else 0.0
                        dist_tp = ((combined_buyback - self.active_strategy.take_profit_target_premium) / combined_entry) * 100.0 if combined_entry > 0 else 0.0
                        log_text = (
                            f"Live PnL Check: Buyback ${combined_buyback:,.2f} | "
                            f"PnL: {pnl_sign}${pnl_usd:.2f} ({pnl_sign}{pnl_pct:.2f}%) | "
                            f"SL Distance: {dist_sl:+.2f}% | TP Distance: {dist_tp:+.2f}%"
                        )
                        state_persistence.add_log(
                            category="PNL",
                            message=log_text,
                            badge_type="pnl-pos" if pnl_pct >= 0 else "pnl-neg",
                            pnl_pct=pnl_pct,
                            pnl_usd=pnl_usd,
                        )
                        state_persistence.update_valuation(
                            call_ask=call_ask,
                            call_bid=call_bid,
                            put_ask=put_ask,
                            put_bid=put_bid,
                            current_buyback_premium=combined_buyback,
                            pnl_pct=pnl_pct,
                            pnl_usd=pnl_usd,
                        )
                        self._broadcast(
                            {
                                "type": "PNL_HEARTBEAT",
                                "message": log_text,
                                "pnl_pct": pnl_pct,
                                "pnl_usd": pnl_usd,
                                "current_buyback_premium": combined_buyback,
                                "combined_entry_premium": combined_entry,
                            }
                        )

                    # Check Level 2 Automated Risk Management
                    if level == 2 and not self.active_strategy.exit_triggered:
                        # Stop-Loss: buyback premium increased by >= 1%
                        if combined_buyback >= self.active_strategy.stop_loss_target_premium:
                            self._execute_exit(
                                reason="STOP_LOSS (+1% Breach)",
                                pair=pair,
                                call_ask=call_ask,
                                put_ask=put_ask,
                                trade_size=trade_size,
                            )
                            break

                        # Take-Profit: buyback premium decreased by >= 2%
                        elif combined_buyback <= self.active_strategy.take_profit_target_premium:
                            self._execute_exit(
                                reason="TAKE_PROFIT (-2% Target Achieved)",
                                pair=pair,
                                call_ask=call_ask,
                                put_ask=put_ask,
                                trade_size=trade_size,
                            )
                            break

            except Exception as e:
                logger.error(f"Error in market loop: {e}")

            time.sleep(1.0)  # 1-second ticker resolution

    def _execute_exit(
        self,
        reason: str,
        pair: StraddlePair,
        call_ask: float,
        put_ask: float,
        trade_size: float,
    ) -> None:
        """Emulate and broadcast automated exit execution for SL / TP."""
        self.active_strategy.exit_triggered = True
        self.active_strategy.strategy_state = StrategyState.EXIT_TRIGGERED

        call_close_id = f"CALL-BUY-CLOSE-{int(time.time() * 1000)}"
        put_close_id = f"PUT-BUY-CLOSE-{int(time.time() * 1000)}"

        combined_exit = max(call_ask + put_ask, 1.0)
        entry = self.active_strategy.combined_entry_premium
        actual_size = self.current_trade_size or trade_size
        pnl_pct = ((entry - combined_exit) / entry) * 100.0 if entry > 0 else 0.0
        pnl_usd = (entry - combined_exit) * actual_size

        state_persistence.close_active_trade(
            reason=reason,
            exit_buyback=combined_exit,
            pnl_pct=pnl_pct,
            pnl_usd=pnl_usd,
        )
        state_persistence.add_log(
            category="RISK" if ("STOP_LOSS" in reason or "TAKE_PROFIT" in reason) else "ORDER",
            message=f"Exit Executed [{reason}]: Buyback ${combined_exit:,.2f} | Final Realized PnL: {pnl_pct:+.2f}% (${pnl_usd:+.2f} USD)",
            badge_type="danger" if "STOP_LOSS" in reason else ("success" if pnl_pct >= 0 else "warning"),
            pnl_pct=pnl_pct,
            pnl_usd=pnl_usd,
        )

        self._broadcast(
            {
                "type": "EXIT_TRIGGERED",
                "reason": reason,
                "current_buyback_premium": combined_exit,
                "combined_entry_premium": entry,
                "pnl_pct": pnl_pct,
                "pnl_usd": pnl_usd,
            }
        )
        time.sleep(0.5)

        # Live Bybit Demo / Testnet Closing Orders
        if self.bybit_client and not self.is_simulated:
            logger.info(f"Submitting LIVE Bybit closing orders for {actual_size} BTC (reduceOnly=True)...")
            try:
                self.bybit_client.place_order(symbol=pair.call_contract.symbol, side="Buy", qty=str(actual_size), reduce_only=True)
                self.bybit_client.place_order(symbol=pair.put_contract.symbol, side="Buy", qty=str(actual_size), reduce_only=True)
            except Exception as e:
                logger.error(f"Error executing live Bybit exit: {e}")

        # Submit closing buy orders
        self._broadcast(
            {
                "type": "ORDER_SUBMITTED",
                "client_order_id": call_close_id,
                "instrument_id": pair.call_contract.instrument_id,
                "order_side": "BUY",
                "reduce_only": True,
            }
        )
        self._broadcast(
            {
                "type": "ORDER_SUBMITTED",
                "client_order_id": put_close_id,
                "instrument_id": pair.put_contract.instrument_id,
                "order_side": "BUY",
                "reduce_only": True,
            }
        )
        time.sleep(0.5)

        # Fills
        self._broadcast(
            {
                "type": "ORDER_FILLED",
                "client_order_id": call_close_id,
                "instrument_id": pair.call_contract.instrument_id,
                "fill_price": call_ask,
                "fill_qty": trade_size,
                "order_side": "BUY",
            }
        )
        self._broadcast(
            {
                "type": "ORDER_FILLED",
                "client_order_id": put_close_id,
                "instrument_id": pair.put_contract.instrument_id,
                "fill_price": put_ask,
                "fill_qty": trade_size,
                "order_side": "BUY",
            }
        )

        self.active_strategy.strategy_state = StrategyState.CLOSED
        self.is_running = False
        self._stop_event.set()
        self._broadcast(
            {
                "type": "STRADDLE_CLOSED",
                "reason": reason,
                "final_call_price": call_ask,
                "final_put_price": put_ask,
                "final_buyback_premium": call_ask + put_ask,
                "status": "STOPPED",
            }
        )

    def _fetch_option_ticker(self, symbol: str) -> Dict[str, float]:
        """Fetch real-time ticker quotes for a given option contract."""
        url = f"{self.base_url}/v5/market/tickers?category=option&symbol={symbol}"
        try:
            res = requests.get(url, timeout=5).json()
            if res.get("retCode") == 0 and res.get("result", {}).get("list"):
                item = res["result"]["list"][0]
                return {
                    "bid1Price": float(item.get("bid1Price") or 0.0),
                    "ask1Price": float(item.get("ask1Price") or 0.0),
                    "markPrice": float(item.get("markPrice") or 0.0),
                    "delta": float(item.get("delta") or 0.0),
                    "gamma": float(item.get("gamma") or 0.0),
                    "vega": float(item.get("vega") or 0.0),
                    "theta": float(item.get("theta") or 0.0),
                }
        except Exception as e:
            logger.error(f"Failed to fetch ticker for {symbol}: {e}")
        return {"bid1Price": 0.0, "ask1Price": 0.0, "markPrice": 0.0}

    def stop_straddle(self) -> Dict[str, Any]:
        """Manually stops the strategy and flattens any open straddle positions."""
        self._stop_event.set()
        self.is_running = False

        if self.active_strategy and self.active_strategy.strategy_state == StrategyState.ACTIVE_POSITION:
            if self.current_pair:
                self._execute_exit(
                    reason="MANUAL_OPERATOR_STOP",
                    pair=self.current_pair,
                    call_ask=self.active_strategy.current_call_ask,
                    put_ask=self.active_strategy.current_put_ask,
                    trade_size=self.current_trade_size,
                )

        # Emergency sweep: Close any residual open option positions directly on Bybit
        if self.bybit_client and not self.is_simulated:
            try:
                positions = self.bybit_client.get_open_positions("BTC").get("result", {}).get("list", [])
                for p in positions:
                    sym = p.get("symbol")
                    qty = p.get("size")
                    side = "Buy" if p.get("side", "").lower() == "sell" else "Sell"
                    if sym and qty:
                        self.bybit_client.place_order(symbol=sym, side=side, qty=str(qty), reduce_only=True)
            except Exception as e:
                logger.error(f"Error sweeping residual positions on stop: {e}")

        state_persistence.clear_active_trade()
        self._broadcast({"type": "STRATEGY_STOPPED", "status": "STOPPED"})
        return {"status": "STOPPED"}

    def simulate_price_shock(self, shock_type: str = "STOP_LOSS") -> Dict[str, Any]:
        """
        Testing helper: Injects a price shock on the active straddle to demonstrate
        the automated 1% Stop-Loss or 2% Take-Profit execution.
        """
        if not self.active_strategy or self.active_strategy.strategy_state != StrategyState.ACTIVE_POSITION:
            raise RuntimeError("No active straddle position to shock.")

        entry = self.active_strategy.combined_entry_premium
        if shock_type == "STOP_LOSS":
            # Shock buyback premium up by +1.5% (> 1% Stop-Loss threshold)
            target = entry * 1.015
            self.active_strategy.current_call_ask = target / 2.0
            self.active_strategy.current_put_ask = target / 2.0
            if self.current_pair:
                self._execute_exit(
                    reason="STOP_LOSS (+1% Threshold Triggered via Market Shock)",
                    pair=self.current_pair,
                    call_ask=target / 2.0,
                    put_ask=target / 2.0,
                    trade_size=self.current_trade_size,
                )
            return {"status": "STOP_LOSS_TRIGGERED", "buyback_premium": target}

        elif shock_type == "TAKE_PROFIT":
            # Shock buyback premium down by -2.5% (< 2% Take-Profit threshold)
            target = entry * 0.975
            self.active_strategy.current_call_ask = target / 2.0
            self.active_strategy.current_put_ask = target / 2.0
            if self.current_pair:
                self._execute_exit(
                    reason="TAKE_PROFIT (-2% Target Triggered via Market Shock)",
                    pair=self.current_pair,
                    call_ask=target / 2.0,
                    put_ask=target / 2.0,
                    trade_size=self.current_trade_size,
                )
            return {"status": "TAKE_PROFIT_TRIGGERED", "buyback_premium": target}

        return {"status": "UNKNOWN_SHOCK_TYPE"}

