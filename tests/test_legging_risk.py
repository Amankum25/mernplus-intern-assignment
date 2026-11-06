"""
Unit tests for Legging Risk / Partial Execution handling.
"""

from decimal import Decimal
import pytest
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId
from straddle.config import BTCShortStraddleConfig
from straddle.strategy import BTCShortStraddleStrategy, StrategyState


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
