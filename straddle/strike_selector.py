"""
================================================================================
⚡ AT-THE-MONEY (ATM) STRIKE SELECTOR (straddle/strike_selector.py)
================================================================================
Discovers listed BTC option contracts, tracks underlying reference prices,
and pairs the Call and Put options sharing the identical At-The-Money (ATM) strike.

Architecture & Interview Notes:
1. Native Adapter Integration:
   - Does NOT use direct external REST clients (`requests`, `urllib`, `ccxt`, `pybit`).
   - Uses NautilusTrader's PyO3 Rust-backed Bybit adapter (`get_cached_bybit_http_client`).
2. Mathematical ATM Resolution:
   - Evaluates spot/index price against listed strikes: K_ATM = argmin |K - S_ref|.
   - Ensures both legs share identical strike price and expiration timestamp (delivery_time_ms).
3. Thread & Loop Marshalling:
   - Bridges sync Rust adapter bindings with FastAPI's asynchronous event loop safely.
================================================================================
"""

import asyncio
import datetime
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from nautilus_trader.adapters.bybit import BybitEnvironment, BybitProductType
from nautilus_trader.adapters.bybit.factories import get_cached_bybit_http_client
from nautilus_trader.core.nautilus_pyo3 import BybitTickersParams
from nautilus_trader.model.instruments import OptionContract
from nautilus_trader.model.identifiers import InstrumentId


@dataclass
class OptionContractInfo:
    """
    Holds metadata for a single Bybit BTC option contract.
    Parsed from Bybit's instruments-info API response via NautilusTrader.
    """
    symbol: str               # Bybit contract symbol (e.g. 'BTC-14NOV26-67000-C-USDT')
    instrument_id: str        # Nautilus ID format (e.g. 'BTC-14NOV26-67000-C-USDT-OPTION.BYBIT')
    strike: float             # Strike price in USD
    option_type: str          # 'CALL' or 'PUT'
    delivery_time_ms: int     # Expiry timestamp in epoch milliseconds (determines expiry pairing)
    expiry_str: str           # Human-readable UTC expiry string (e.g. '2026-11-14 08:00 UTC')
    min_order_qty: float      # Minimum trade size (0.01 BTC on Bybit)
    price_tick_size: float    # Smallest price increment for quoting ($5.00)


@dataclass
class StraddlePair:
    """
    Result of ATM resolution — holds paired Call and Put legs sharing the same strike and expiry.
    """
    btc_price: float
    expiry_ms: int
    expiry_str: str
    atm_strike: float
    call_contract: OptionContractInfo
    put_contract: OptionContractInfo
    available_strikes: List[float] = field(default_factory=list)
    available_expiries: List[Dict[str, Any]] = field(default_factory=list)
    strike_matrix: List[Dict[str, Any]] = field(default_factory=list)
    selected_strike: float = 0.0
    available_contracts_count: int = 0


class BybitStrikeSelector:
    """
    Uses NautilusTrader's native Bybit adapter to discover BTC option instruments
    and query reference/option ticker data. This module has no HTTP/REST client of its own.
    """

    def __init__(
        self,
        environment: str = "DEMO",
        api_key: str = "",
        api_secret: str = "",
        event_loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        environments = {
            "DEMO": BybitEnvironment.DEMO,
            "TESTNET": BybitEnvironment.TESTNET,
            "MAINNET": BybitEnvironment.MAINNET,
        }
        self.environment = environments.get(environment.upper(), BybitEnvironment.DEMO)
        self._api_key = api_key or os.getenv("BYBIT_API_KEY", "")
        self._api_secret = api_secret or os.getenv("BYBIT_API_SECRET", "")
        self._event_loop = event_loop
        self.http_client = get_cached_bybit_http_client(
            environment=self.environment,
            api_key=self._api_key,
            api_secret=self._api_secret,
            timeout_secs=8,
            max_retries=0,
            retry_delay_ms=100,
            retry_delay_max_ms=250,
        )
        self._cached_contracts: List[OptionContractInfo] = []
        self._cached_contracts_ts: float = 0.0
        self._cached_price: float = 0.0
        self._cached_price_ts: float = 0.0

    def set_event_loop(self, event_loop: asyncio.AbstractEventLoop) -> None:
        """Use the FastAPI/TradingNode host loop for adapter I/O when available."""
        self._event_loop = event_loop

    def _run_native(self, operation):
        """Run a native Bybit adapter call on the configured application event loop.

        The Rust-backed Bybit HTTP helper requires an active asyncio loop even though
        its public methods are synchronous. FastAPI invokes sync endpoints in worker
        threads, so marshal the operation onto the loop captured at app startup.
        """
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        loop = self._event_loop
        if loop is None or not loop.is_running():
            # If the caller already runs inside an event loop, execute there rather
            # than creating a second loop around NautilusTrader's native client.
            if current_loop is not None:
                return operation()
            raise RuntimeError(
                "NautilusTrader Bybit market reads require the running application event loop; "
                "start the dashboard before requesting market data."
            )

        if current_loop is loop:
            # This is only a non-blocking native callback when called from the owning
            # loop. FastAPI market routes are synchronous and run in worker threads.
            return operation()

        async def invoke():
            result = operation()
            if hasattr(result, "__await__"):
                return await result
            return result

        future = asyncio.run_coroutine_threadsafe(invoke(), loop)
        try:
            # There are up to four serial reads (reference, instruments, two quotes),
            # each configured with an 8 second adapter timeout.
            return future.result(timeout=40)
        except TimeoutError as exc:
            future.cancel()
            raise TimeoutError("NautilusTrader Bybit market-data request timed out after 40 seconds.") from exc

    def _request_tickers(self, product_type: BybitProductType, symbol: Optional[str] = None, base_coin: Optional[str] = None):
        params = BybitTickersParams(category=product_type, symbol=symbol, base_coin=base_coin)
        return self._run_native(lambda: self.http_client.request_tickers(params))

    def get_ticker(self, symbol: str, product_type: BybitProductType = BybitProductType.OPTION) -> Dict[str, float]:
        """Return normalized ticker fields from NautilusTrader's native Bybit adapter."""
        tickers = self._request_tickers(product_type, symbol=symbol)
        if not tickers:
            return {
                "bid1Price": 0.0,
                "ask1Price": 0.0,
                "bid1Size": 0.0,
                "ask1Size": 0.0,
                "markPrice": 0.0,
                "lastPrice": 0.0,
                "indexPrice": 0.0,
            }
        ticker = tickers[0]
        return {
            "bid1Price": float(ticker.bid1_price or 0.0),
            "ask1Price": float(ticker.ask1_price or 0.0),
            "bid1Size": float(ticker.bid1_size or 0.0),
            "ask1Size": float(ticker.ask1_size or 0.0),
            "markPrice": float(ticker.mark_price or 0.0),
            "lastPrice": float(ticker.last_price or 0.0),
            "indexPrice": float(ticker.index_price or 0.0),
        }

    def get_btc_reference_price(self) -> float:
        """
        Gets the current BTC price from Bybit.
        Reads the BTCUSDT linear ticker through NautilusTrader's Bybit adapter.
        Cached for 2.5 seconds so we don't hammer the API on every tick.
        """
        now = time.time()
        if self._cached_price > 0 and (now - self._cached_price_ts) < 2.5:
            return self._cached_price

        try:
            tickers = self._request_tickers(BybitProductType.LINEAR, symbol="BTCUSDT")
            if tickers:
                ticker = tickers[0]
                price = float(ticker.last_price or ticker.index_price or ticker.mark_price or 0.0)
                if price > 0:
                    self._cached_price = price
                    self._cached_price_ts = now
                    return price
        except Exception as e:
            print(f"[StrikeSelector] Failed to fetch linear BTCUSDT price: {e}")

        try:
            tickers = self._request_tickers(BybitProductType.OPTION, base_coin="BTC")
            for ticker in tickers or []:
                ref = float(ticker.index_price or 0.0)
                if ref > 0:
                    self._cached_price = ref
                    self._cached_price_ts = now
                    return ref
        except Exception as e:
            print(f"[StrikeSelector] Failed to fetch option tickers: {e}")

        raise RuntimeError("NautilusTrader's Bybit adapter could not provide a live BTC reference price.")

    def get_all_option_contracts(self) -> List[OptionContractInfo]:
        """
        Fetches all active BTC option contracts from Bybit.
        Cached for 45 seconds — the options chain doesn't change that fast.
        """
        now = time.time()
        if self._cached_contracts and (now - self._cached_contracts_ts) < 45.0:
            return self._cached_contracts

        try:
            instruments = self._run_native(lambda: self.http_client.request_instruments(BybitProductType.OPTION, None, "BTC"))
            if not instruments:
                if self._cached_contracts:
                    return self._cached_contracts
                raise RuntimeError("NautilusTrader's Bybit adapter returned no BTC option instruments.")
            now_ms = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
            contracts: List[OptionContractInfo] = []

            for instrument in instruments:
                if not hasattr(instrument, "expiration_ns") or not hasattr(instrument, "raw_symbol"):
                    continue
                delivery_time_ms = int(instrument.expiration_ns // 1_000_000)
                # skip expired contracts
                if delivery_time_ms <= now_ms:
                    continue

                raw_symbol = str(instrument.raw_symbol)  # native instrument's Bybit symbol
                options_type = "CALL" if "-C-" in raw_symbol else "PUT" if "-P-" in raw_symbol else ""
                if not options_type:
                    continue

                # parse strike from the symbol name (3rd segment after splitting by "-")
                parts = raw_symbol.split("-")
                strike = 0.0
                if len(parts) >= 3:
                    try:
                        strike = float(parts[2])
                    except ValueError:
                        strike = instrument.strike_price.as_double()
                else:
                    strike = instrument.strike_price.as_double()

                if strike <= 0:
                    continue

                instrument_id = str(instrument.id)
                exp_dt = datetime.datetime.fromtimestamp(delivery_time_ms / 1000, datetime.timezone.utc)
                expiry_str = exp_dt.strftime("%Y-%m-%d %H:%M UTC")

                min_order_qty = instrument.min_quantity.as_double()
                price_tick_size = instrument.price_increment.as_double()

                contracts.append(
                    OptionContractInfo(
                        symbol=raw_symbol,
                        instrument_id=instrument_id,
                        strike=strike,
                        option_type=options_type,
                        delivery_time_ms=delivery_time_ms,
                        expiry_str=expiry_str,
                        min_order_qty=min_order_qty,
                        price_tick_size=price_tick_size,
                    )
                )

            if contracts:
                self._cached_contracts = contracts
                self._cached_contracts_ts = now
                return contracts

        except Exception as e:
            if self._cached_contracts:
                return self._cached_contracts
            raise e

        return self._cached_contracts

    def select_atm_straddle(
        self,
        target_expiry_ms: Optional[int] = None,
        target_strike: Optional[float] = None,
        override_btc_price: Optional[float] = None,
    ) -> StraddlePair:
        """
        Core method: finds the nearest ATM strike and returns the matched Call + Put pair.

        ATM selection uses: min(strikes, key=lambda s: abs(s - btc_price))
        That just means "pick the strike closest to the current BTC price."
        """
        btc_price = override_btc_price if override_btc_price is not None else self.get_btc_reference_price()
        contracts = self.get_all_option_contracts()

        if not contracts:
            raise RuntimeError("No active BTC option contracts found on Bybit.")

        # collect unique expiry dates
        expiry_map: Dict[int, str] = {}
        for c in contracts:
            if c.delivery_time_ms not in expiry_map:
                expiry_map[c.delivery_time_ms] = c.expiry_str

        sorted_expiries = sorted(expiry_map.keys())
        available_expiries = [{"timestamp": exp, "label": expiry_map[exp]} for exp in sorted_expiries]

        # default to nearest expiry if none specified
        if target_expiry_ms and target_expiry_ms in expiry_map:
            chosen_expiry = target_expiry_ms
        else:
            chosen_expiry = sorted_expiries[0]

        contracts_at_expiry = [c for c in contracts if c.delivery_time_ms == chosen_expiry]
        strikes = sorted(list(set(c.strike for c in contracts_at_expiry)))

        if not strikes:
            raise RuntimeError(f"No strikes found for expiry {expiry_map[chosen_expiry]}")

        # pick the strike mathematically closest to current BTC price
        atm_strike = min(strikes, key=lambda s: abs(s - btc_price))
        chosen_strike = target_strike if (target_strike is not None and target_strike in strikes) else atm_strike

        # find the Call and Put contracts at this strike
        call_contract = next(
            (c for c in contracts_at_expiry if c.strike == chosen_strike and c.option_type == "CALL"),
            None,
        )
        put_contract = next(
            (c for c in contracts_at_expiry if c.strike == chosen_strike and c.option_type == "PUT"),
            None,
        )

        # a straddle must have BOTH legs — if either is missing something is wrong with the data
        if not call_contract or not put_contract:
            raise RuntimeError(
                f"Could not find matching Call and Put for strike {chosen_strike} at expiry {expiry_map[chosen_expiry]}"
            )

        # build a ±3 strike matrix around ATM for the UI strike selector
        atm_idx = strikes.index(atm_strike)
        start_idx = max(0, atm_idx - 3)
        end_idx = min(len(strikes), atm_idx + 4)
        strike_matrix = []
        for s in strikes[start_idx:end_idx]:
            c_sym = next((c.symbol for c in contracts_at_expiry if c.strike == s and c.option_type == "CALL"), None)
            p_sym = next((c.symbol for c in contracts_at_expiry if c.strike == s and c.option_type == "PUT"), None)
            strike_matrix.append(
                {
                    "strike": s,
                    "distance": round(abs(s - btc_price), 2),
                    "diff": round(s - btc_price, 2),
                    "is_atm": s == atm_strike,
                    "is_selected": s == chosen_strike,
                    "call_symbol": c_sym,
                    "put_symbol": p_sym,
                }
            )

        return StraddlePair(
            btc_price=btc_price,
            expiry_ms=chosen_expiry,
            expiry_str=expiry_map[chosen_expiry],
            atm_strike=atm_strike,
            call_contract=call_contract,
            put_contract=put_contract,
            available_strikes=strikes,
            available_expiries=available_expiries,
            strike_matrix=strike_matrix,
            selected_strike=chosen_strike,
            available_contracts_count=len(contracts_at_expiry),
        )
