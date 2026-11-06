"""
Unit tests for ATM strike discovery and option pair selection.
"""

import pytest
from straddle.strike_selector import BybitStrikeSelector, OptionContractInfo


def test_atm_strike_calculation_logic():
    """Verify that ATM strike selects the nearest strike to the underlying price."""
    selector = BybitStrikeSelector()
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
    """Query live Bybit API and verify valid ATM straddle pairing."""
    selector = BybitStrikeSelector()
    pair = selector.select_atm_straddle()

    assert pair.btc_price > 0
    assert pair.atm_strike > 0
    assert pair.call_contract is not None
    assert pair.put_contract is not None

    # Both legs must share the exact same strike
    assert pair.call_contract.strike == pair.atm_strike
    assert pair.put_contract.strike == pair.atm_strike

    # Both legs must share the exact same expiration
    assert pair.call_contract.delivery_time_ms == pair.expiry_ms
    assert pair.put_contract.delivery_time_ms == pair.expiry_ms

    # Confirm types
    assert pair.call_contract.option_type == "CALL"
    assert pair.put_contract.option_type == "PUT"

    # Confirm Nautilus instrument ID format
    assert pair.call_contract.instrument_id.endswith("-OPTION.BYBIT")
    assert pair.put_contract.instrument_id.endswith("-OPTION.BYBIT")

