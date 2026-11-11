"""
All strategy parameters live here as a frozen dataclass.
frozen=True means NautilusTrader can hash the config and prevent accidental mutation while the strategy is running.
"""

from decimal import Decimal
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.identifiers import InstrumentId


class BTCShortStraddleConfig(StrategyConfig, frozen=True):
    """
    Config object passed into the strategy at startup.
    We use Decimal instead of float for all monetary values — float has rounding errors
    that compound badly in financial calculations.

    level 1 = naked straddle (hold indefinitely, no auto-exit)
    level 2 = automated 1% stop-loss / 2% take-profit
    """

    call_instrument_id: InstrumentId   # e.g., BTC-28MAR25-85000-C-USDT-OPTION.BYBIT
    put_instrument_id: InstrumentId    # same strike and expiry, but Put side

    trade_size: Decimal = Decimal("0.01")       # 0.01 BTC per leg — Bybit's minimum
    level: int = 2                              # 2 enables automated SL/TP

    stop_loss_pct: Decimal = Decimal("0.01")    # exit if buyback cost rises 1% from entry
    take_profit_pct: Decimal = Decimal("0.02")  # exit if buyback cost falls 2% from entry

    entry_timeout_secs: float = 10.0            # give up waiting for leg fill after this many seconds
    simulated_execution: bool = False           # True = emulate fills locally using live quotes (no real orders)
