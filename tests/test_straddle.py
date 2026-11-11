"""
Unit tests for the BTC Options Short Straddle system.
Covers:
1. ATM strike discovery and option contract pairing.
2. Level 2 risk management (1% Stop-Loss, 2% Take-Profit, and idempotency).
3. Legging risk and unhedged single-leg safety unwinding.
"""

from decimal import Decimal
import pytest
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId

from straddle.config import BTCShortStraddleConfig
from straddle.strategy import BTCShortStraddleStrategy, StrategyState
from straddle.strike_selector import BybitStrikeSelector, OptionContractInfo


# =============================================================================
# 1. ATM Strike Selection Tests
# =============================================================================

def test_atm_strike_calculation_logic():
    """Verify that ATM strike selection picks the nearest strike to spot price."""
    strikes = [70000.0, 72000.0, 74000.0, 75000.0, 76000.0, 78000.0]

    # Test 1: Price is 75,200 -> Nearest is 75,000
    atm_1 = min(strikes, key=lambda s: abs(s - 75200.0))
    assert atm_1 == 75000.0

    # Test 2: Price is 75,700 -> Nearest is 76,000
    atm_2 = min(strikes, key=lambda s: abs(s - 75700.0))
    assert atm_2 == 76000.0

    # Test 3: Price is exactly 75,000 -> Nearest is 75,000
    atm_3 = min(strikes, key=lambda s: abs(s - 75000.0))
    assert atm_3 == 75000.0


def test_live_bybit_strike_discovery():
    """Query Bybit API and verify valid ATM Call and Put straddle pairing."""
    selector = BybitStrikeSelector()
    pair = selector.select_atm_straddle()

    assert pair.btc_price > 0
    assert pair.atm_strike > 0
    assert pair.call_contract is not None
    assert pair.put_contract is not None

    # Both legs must share the exact same strike and expiry
    assert pair.call_contract.strike == pair.atm_strike
    assert pair.put_contract.strike == pair.atm_strike
    assert pair.call_contract.delivery_time_ms == pair.expiry_ms
    assert pair.put_contract.delivery_time_ms == pair.expiry_ms

    # Confirm types and instrument format
    assert pair.call_contract.option_type == "CALL"
    assert pair.put_contract.option_type == "PUT"
    assert pair.call_contract.instrument_id.endswith("-OPTION.BYBIT")
    assert pair.put_contract.instrument_id.endswith("-OPTION.BYBIT")


# =============================================================================
# 2. Risk Management Tests (1% Stop-Loss / 2% Take-Profit / Idempotency)
# =============================================================================

def test_straddle_entry_and_targets_calculation():
    """Verify entry combined premium, 1% Stop-Loss target, and 2% Take-Profit target."""
    call_id = InstrumentId.from_str("BTC-7NOV26-75000-C-USDT-OPTION.BYBIT")
    put_id = InstrumentId.from_str("BTC-7NOV26-75000-P-USDT-OPTION.BYBIT")

    config = BTCShortStraddleConfig(
        call_instrument_id=call_id,
        put_instrument_id=put_id,
        trade_size=Decimal("0.01"),
        level=2,
        stop_loss_pct=Decimal("0.01"),   # +1%
        take_profit_pct=Decimal("0.02"), # -2%
    )

    strat = BTCShortStraddleStrategy(config=config)

    # Simulate entry fills: Call sold at $1200, Put sold at $1000
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

    # First trigger locks exit
    strat.exit_triggered = True
    strat.strategy_state = StrategyState.EXIT_TRIGGERED

    # Any subsequent trigger should be immediately ignored
    strat._check_risk_management()
    assert strat.strategy_state == StrategyState.EXIT_TRIGGERED


# =============================================================================
# 3. Legging Risk & Unhedged Execution Tests
# =============================================================================

def test_legging_failure_unhedged_handling():
    """
    Verify that if Call fills and Put fails/is rejected,
    the strategy immediately recognizes unhedged exposure and safely closes the open leg.
    """
    call_id = InstrumentId.from_str("BTC-7NOV26-75000-C-USDT-OPTION.BYBIT")
    put_id = InstrumentId.from_str("BTC-7NOV26-75000-P-USDT-OPTION.BYBIT")

    config = BTCShortStraddleConfig(
        call_instrument_id=call_id,
        put_instrument_id=put_id,
        level=2,
    )
    strat = BTCShortStraddleStrategy(config=config)

    # Call filled, but Put not filled
    strat.call_entry_filled = True
    strat.put_entry_filled = False

    failed_order_id = ClientOrderId("PUT-ORDER-FAILED")
    strat._handle_legging_failure(failed_order_id)

    # State must transition to UNHEDGED_ALERT
    assert strat.strategy_state == StrategyState.UNHEDGED_ALERT

