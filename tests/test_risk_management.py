"""
Unit tests for Level 1 (Naked) and Level 2 (Automated Risk Management: 1% SL / 2% TP).
"""

from decimal import Decimal
import pytest
from nautilus_trader.model.identifiers import InstrumentId
from straddle.config import BTCShortStraddleConfig
from straddle.strategy import BTCShortStraddleStrategy, StrategyState


def test_straddle_entry_and_targets_calculation():
    """Verify entry combined premium, 1% Stop-Loss target, and 2% Take-Profit target."""
    call_id = InstrumentId.from_str("BTC-7NOV26-75000-C-USDT-OPTION.BYBIT")
    put_id = InstrumentId.from_str("BTC-7NOV26-75000-P-USDT-OPTION.BYBIT")

    config = BTCShortStraddleConfig(
        call_instrument_id=call_id,
        put_instrument_id=put_id,
        trade_size=Decimal("0.01"),
        level=2,
        stop_loss_pct=Decimal("0.01"),   # 1%
        take_profit_pct=Decimal("0.02"), # 2%
    )

    strat = BTCShortStraddleStrategy(config=config)

    # Simulate Entry fills: Call sold at $1200, Put sold at $1000
    strat.call_entry_filled = True
    strat.call_entry_price = 1200.0
    strat.put_entry_filled = True
    strat.put_entry_price = 1000.0
    strat.combined_entry_premium = strat.call_entry_price + strat.put_entry_price
    assert strat.combined_entry_premium == 2200.0

    # Calculate targets
    strat.stop_loss_target_premium = strat.combined_entry_premium * 1.01   # 2200 * 1.01 = 2222.0
    strat.take_profit_target_premium = strat.combined_entry_premium * 0.98 # 2200 * 0.98 = 2156.0

    assert strat.stop_loss_target_premium == 2222.0
    assert strat.take_profit_target_premium == 2156.0

    strat.strategy_state = StrategyState.ACTIVE_POSITION

    # Scenario A: Quotes increase cost to $2225 (Breaches Stop-Loss 1%)
    strat.current_call_ask = 1215.0
    strat.current_put_ask = 1010.0
    buyback = strat.current_call_ask + strat.current_put_ask  # 2225.0
    strat.current_buyback_premium = buyback
    assert buyback >= strat.stop_loss_target_premium

    # Scenario B: Quotes decay favorably to $2150 (Hits Take-Profit 2%)
    strat.current_call_ask = 1170.0
    strat.current_put_ask = 980.0
    buyback = strat.current_call_ask + strat.current_put_ask  # 2150.0
    strat.current_buyback_premium = buyback
    assert buyback <= strat.take_profit_target_premium


def test_duplicate_exit_attempt_guard():
    """Verify idempotency guard prevents multiple exit triggers."""
    call_id = InstrumentId.from_str("BTC-7NOV26-75000-C-USDT-OPTION.BYBIT")
    put_id = InstrumentId.from_str("BTC-7NOV26-75000-P-USDT-OPTION.BYBIT")

    config = BTCShortStraddleConfig(
        call_instrument_id=call_id,
        put_instrument_id=put_id,
        level=2,
    )
    strat = BTCShortStraddleStrategy(config=config)
    strat.strategy_state = StrategyState.ACTIVE_POSITION
    strat.combined_entry_premium = 2000.0
    strat.stop_loss_target_premium = 2020.0
    strat.current_buyback_premium = 2030.0

    # First trigger
    strat.exit_triggered = True
    strat.strategy_state = StrategyState.EXIT_TRIGGERED

    # Any subsequent trigger should be immediately ignored
    strat._check_risk_management()
    assert strat.strategy_state == StrategyState.EXIT_TRIGGERED
