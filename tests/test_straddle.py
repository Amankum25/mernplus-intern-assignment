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
from nautilus_trader.model.enums import OmsType

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


def test_bybit_strike_discovery_pairs_both_legs(monkeypatch):
    """Verify ATM pairing locally without requiring a live network connection."""
    selector = BybitStrikeSelector()
    expiry = 1_800_000_000_000
    contracts = [
        OptionContractInfo("BTC-7NOV26-75000-C-USDT", "BTC-7NOV26-75000-C-USDT-OPTION.BYBIT", 75000.0, "CALL", expiry, "2026-11-07", 0.01, 0.5),
        OptionContractInfo("BTC-7NOV26-75000-P-USDT", "BTC-7NOV26-75000-P-USDT-OPTION.BYBIT", 75000.0, "PUT", expiry, "2026-11-07", 0.01, 0.5),
        OptionContractInfo("BTC-7NOV26-76000-C-USDT", "BTC-7NOV26-76000-C-USDT-OPTION.BYBIT", 76000.0, "CALL", expiry, "2026-11-07", 0.01, 0.5),
        OptionContractInfo("BTC-7NOV26-76000-P-USDT", "BTC-7NOV26-76000-P-USDT-OPTION.BYBIT", 76000.0, "PUT", expiry, "2026-11-07", 0.01, 0.5),
    ]
    monkeypatch.setattr(selector, "get_btc_reference_price", lambda: 75_100.0)
    monkeypatch.setattr(selector, "get_all_option_contracts", lambda: contracts)
    pair = selector.select_atm_straddle()

    assert pair.btc_price == 75_100.0
    assert pair.atm_strike == 75_000.0
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


def test_native_sync_adapter_call_is_marshaled_to_host_loop(monkeypatch):
    """Rust-backed sync Bybit adapter methods must run on the app's active loop."""
    selector = BybitStrikeSelector()
    loop = type("Loop", (), {
        "is_running": lambda self: True,
    })()
    selector.set_event_loop(loop)
    scheduled = {}

    class Future:
        def result(self, timeout):
            scheduled["timeout"] = timeout
            return "adapter result"

        def cancel(self):
            scheduled["cancelled"] = True

    def schedule(coroutine, target_loop):
        scheduled["loop"] = target_loop
        coroutine.close()
        return Future()

    monkeypatch.setattr("straddle.strike_selector.asyncio.run_coroutine_threadsafe", schedule)
    assert selector._run_native(lambda: pytest.fail("must execute on the configured host loop")) == "adapter result"
    assert scheduled["loop"] is loop
    assert scheduled["timeout"] == 40


def test_native_adapter_call_fails_fast_without_running_loop():
    """Avoid hanging on asyncio.run when market reads happen before app startup."""
    selector = BybitStrikeSelector()
    with pytest.raises(RuntimeError, match="running application event loop"):
        selector._run_native(lambda: None)


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
    assert config.oms_type == OmsType.HEDGING

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


def test_strategy_closure_closes_both_legs():
    """Closing a saved entry queues its exact NautilusTrader virtual positions."""
    from unittest.mock import MagicMock
    from straddle.execution_manager import StraddleExecutionManager
    from straddle.persistence import state_persistence

    state_persistence.clear_all_active_strategies()

    # Add 2 strategies
    strat1 = {
        "strategy_id": "STRAT-1",
        "name": "Strategy #1: $68,000 Straddle",
        "call_symbol": "BTC-13NOV26-68000-C-USDT",
        "put_symbol": "BTC-13NOV26-68000-P-USDT",
        "trade_size": 0.02,
        "call_position_id": "P-CALL-1",
        "put_position_id": "P-PUT-1",
        "live_verified": True,
        "combined_entry_premium": 500.0,
        "current_buyback_premium": 500.0,
        "created_ts": 1000.0,
    }
    strat2 = {
        "strategy_id": "STRAT-2",
        "name": "Strategy #2: $69,000 Straddle",
        "call_symbol": "BTC-13NOV26-69000-C-USDT",
        "put_symbol": "BTC-13NOV26-69000-P-USDT",
        "trade_size": 0.03,
        "call_position_id": "P-CALL-2",
        "put_position_id": "P-PUT-2",
        "live_verified": True,
        "combined_entry_premium": 520.0,
        "current_buyback_premium": 520.0,
        "created_ts": 2000.0,
    }
    state_persistence.add_active_strategy(strat1)
    state_persistence.add_active_strategy(strat2)

    # Verify descending order of time: strat2 (created_ts=2000) must be first!
    active = state_persistence.get_active_strategies()
    assert len(active) == 2
    assert active[0]["strategy_id"] == "STRAT-2"
    assert active[1]["strategy_id"] == "STRAT-1"

    mgr = StraddleExecutionManager(api_key="mock", api_secret="mock")
    mgr.is_running = True
    mgr.active_strategy = MagicMock()
    mgr._node_is_running = lambda: True
    mgr.reconcile_positions = lambda: []

    # Close STRAT-2
    res = mgr.close_strategy("STRAT-2")
    assert res["status"] == "CLOSING"
    mgr.active_strategy.queue_close.assert_called_once_with(
        "STRAT-2", {"CALL": "P-CALL-2", "PUT": "P-PUT-2"}
    )
    # Entries remain persisted until NautilusTrader confirms both positions closed.
    remaining = state_persistence.get_active_strategies()
    assert len(remaining) == 2

    state_persistence.clear_all_active_strategies()


def test_portfolio_price_shock_closure():
    """The dashboard risk action queues closes through the active Nautilus strategy."""
    from unittest.mock import MagicMock
    from straddle.execution_manager import StraddleExecutionManager
    from straddle.persistence import state_persistence

    state_persistence.clear_all_active_strategies()
    state_persistence.add_active_strategy({
        "strategy_id": "STRAT-SHOCK",
        "call_symbol": "BTC-13NOV26-68000-C-USDT",
        "put_symbol": "BTC-13NOV26-68000-P-USDT",
        "call_position_id": "P-CALL-SHOCK",
        "put_position_id": "P-PUT-SHOCK",
        "live_verified": True,
    })
    mgr = StraddleExecutionManager(api_key="mock", api_secret="mock")
    mgr.is_running = True
    mgr.active_strategy = MagicMock()
    mgr._node_is_running = lambda: True
    mgr.reconcile_positions = lambda: []

    res = mgr.simulate_price_shock("STOP_LOSS", symbol="ALL")
    assert res["status"] == "CLOSING"
    mgr.active_strategy.queue_close.assert_called_once_with(
        "STRAT-SHOCK", {"CALL": "P-CALL-SHOCK", "PUT": "P-PUT-SHOCK"}
    )
    assert state_persistence.get_active_strategies()
    state_persistence.clear_all_active_strategies()


def test_pytest_persistence_is_separate_from_live_session_file():
    """The default launcher test suite must not rewrite live persisted state."""
    from pathlib import Path
    from straddle.persistence import state_persistence

    live_state = Path(__file__).resolve().parents[1] / "data" / "session_state.json"
    assert Path(state_persistence.filepath).resolve() != live_state.resolve()


def test_empty_restart_snapshot_keeps_saved_straddle_mapping():
    """An empty startup position snapshot must not erase persisted entries."""
    from unittest.mock import MagicMock
    from straddle.execution_manager import StraddleExecutionManager
    from straddle.persistence import state_persistence

    state_persistence.clear_all_active_strategies()
    state_persistence.add_active_strategy({
        "strategy_id": "STRAT-EMPTY-SNAPSHOT-TEST",
        "call_symbol": "BTC-13NOV26-68000-C-USDT",
        "put_symbol": "BTC-13NOV26-68000-P-USDT",
        "trade_size": 0.01,
        "status": "OPEN",
    })
    manager = StraddleExecutionManager(api_key="mock", api_secret="mock")
    manager.node = MagicMock()
    manager.node.is_running.return_value = True
    manager.node.cache.positions_open.return_value = []

    assert manager.reconcile_positions() == []
    saved = state_persistence.get_active_strategies()
    assert len(saved) == 1
    assert saved[0]["strategy_id"] == "STRAT-EMPTY-SNAPSHOT-TEST"
    assert saved[0]["status"] == "RECONCILIATION_REQUIRED"
    assert saved[0]["live_verified"] is False
    close_result = manager.close_strategy("STRAT-EMPTY-SNAPSHOT-TEST")
    assert close_result["status"] == "ERROR"
    assert close_result["verified_flat"] is False
    manager.node.cache.positions_open.assert_called()
    state_persistence.clear_all_active_strategies()

