"""
================================================================================
⚡ STRADDLE CONFIGURATION MODULE (straddle/config.py)
================================================================================
Institutional quantitative trading parameters for the BTC Options Short Straddle.

Architecture & Design Highlights (Interview Notes):
1. Inherits from NautilusTrader's `StrategyConfig`:
   - Enforces `frozen=True`: Guarantees immutability at runtime. Once the strategy
     boots up, parameters cannot be accidentally altered by concurrent threads or
     web requests, preventing race conditions.
   - Enforces `kw_only=True`: Forces explicit keyword arguments when instantiating,
     eliminating ordering bugs for monetary and threshold parameters.
2. Decimal Precision:
   - Uses `decimal.Decimal` instead of Python `float` for prices, lot sizes, and
     risk percentages. Floating-point binary representation errors (e.g. 0.1 + 0.2 != 0.3)
     can lead to rejected orders or inaccurate risk boundaries on derivatives venues.
3. Order Management System Mode (`OmsType.HEDGING`):
   - Bybit Unified Trading Account (UTA) nets contracts at the venue level, but
     NautilusTrader's hedging OMS tracks individual entries as separate virtual
     positions, allowing independent Stop-Loss and Take-Profit tracking per entry.
================================================================================
"""

from decimal import Decimal
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import InstrumentId


class BTCShortStraddleConfig(StrategyConfig, frozen=True, kw_only=True):
    """
    Configuration specification passed into BTCShortStraddleStrategy at initialization.
    """

    # --- Core Instrument Identifiers ---
    # Call option contract ID in Nautilus format (e.g. 'BTC-14NOV26-67000-C-USDT-OPTION.BYBIT')
    call_instrument_id: InstrumentId
    # Put option contract ID sharing the exact same strike and delivery expiry
    put_instrument_id: InstrumentId

    # --- Sizing & Strategy Level ---
    # Order quantity per leg in BTC (Bybit V5 options minimum lot size is 0.01 BTC)
    trade_size: Decimal = Decimal("0.01")
    # Strategy mode:
    # Level 1 = Naked Straddle (manual discretion, operator observes and exits manually)
    # Level 2 = Automated Risk Management (sub-second 1% SL and 2% TP bounds)
    level: int = 2

    # --- Risk Management Bounds (Level 2) ---
    # Stop-Loss: Exit if total buyback cost rises 1% above entry premium received (+1%)
    stop_loss_pct: Decimal = Decimal("0.01")
    # Take-Profit: Exit if total buyback cost drops 2% below entry premium received (-2%)
    take_profit_pct: Decimal = Decimal("0.02")

    # --- Defensive Timing & Execution Behavior ---
    # Maximum wait time (in seconds) for both legs to fill; if only one leg fills,
    # the legging risk handler triggers an emergency unwind to prevent unhedged exposure.
    entry_timeout_secs: float = 10.0
    # Simulation flag for local dry-runs without live broker orders
    simulated_execution: bool = False
    # If False, node boots up and waits for explicit entry queuing from dashboard/CLI
    auto_start_entry: bool = True
    # Hedging OMS maintains distinct entry lifecycle tracking in NautilusTrader's cache
    oms_type: OmsType = OmsType.HEDGING
