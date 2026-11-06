"""
Configuration schema for the BTC Options Short Straddle Strategy.
Supports Level 1 (Naked Straddle) and Level 2 (1% SL / 2% TP Automated Risk Management).
"""

from decimal import Decimal
from typing import Any, Callable, Optional

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.identifiers import InstrumentId


class BTCShortStraddleConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``BTCShortStraddleStrategy`` instances.

    Parameters
    ----------
    call_instrument_id : InstrumentId
        The instrument ID of the ATM Call option.
    put_instrument_id : InstrumentId
        The instrument ID of the ATM Put option.
    trade_size : Decimal, default 0.01
        The order quantity per leg (in BTC, min 0.01).
    level : int, default 2
        Strategy mode:
        - 1: Level 1 (Naked Straddle - no SL, no TP).
        - 2: Level 2 (Automated Risk Management with 1% SL and 2% TP).
    stop_loss_pct : Decimal, default 0.01
        Stop-loss threshold percentage (1.0% = 0.01).
        Triggered when buyback combined premium increases by 1% from entry.
    take_profit_pct : Decimal, default 0.02
        Take-profit threshold percentage (2.0% = 0.02).
        Triggered when buyback combined premium decreases by 2% from entry.
    entry_timeout_secs : float, default 10.0
        Safety timeout for straddle entry. If one leg fills but the other leg
        does not fill within this time, the unfilled order is cancelled and the
        filled leg is immediately closed to eliminate directional exposure.
    simulated_execution : bool, default False
        If True, emulates fills internally using live top-of-book quotes
        (useful for testnet when option liquidity/matching is inactive).
    """

    call_instrument_id: InstrumentId
    put_instrument_id: InstrumentId
    trade_size: Decimal = Decimal("0.01")
    level: int = 2
    stop_loss_pct: Decimal = Decimal("0.01")  # 1% Stop-Loss
    take_profit_pct: Decimal = Decimal("0.02")  # 2% Take-Profit
    entry_timeout_secs: float = 10.0
    simulated_execution: bool = False

