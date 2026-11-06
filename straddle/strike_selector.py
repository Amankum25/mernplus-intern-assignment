"""
BTC Options ATM Strike Selector
Identifies available Bybit BTC option contracts, queries the BTC reference price,
and determines the nearest At-The-Money (ATM) strike to pair Call and Put options.
"""

import datetime
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import requests

from nautilus_trader.model.identifiers import InstrumentId


@dataclass
class OptionContractInfo:
    symbol: str
    instrument_id: str
    strike: float
    option_type: str  # "CALL" or "PUT"
    delivery_time_ms: int
    expiry_str: str
    min_order_qty: float
    price_tick_size: float


@dataclass
class StraddlePair:
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


class BybitStrikeSelector:
    """
    Identifies available Bybit BTC option contracts, discovers the nearest
    ATM strike based on the BTC reference price, and pairs matching Call and Put contracts.
    """

    def __init__(self, base_url: str = "https://api.bybit.com"):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self._cached_contracts: List[OptionContractInfo] = []
        self._cached_contracts_ts: float = 0.0
        self._cached_price: float = 0.0
        self._cached_price_ts: float = 0.0

    def get_btc_reference_price(self) -> float:
        """
        Fetch the current BTC underlying reference price from Bybit.
        Uses BTCUSDT linear ticker last/index price or option index price.
        """
        now = time.time()
        if self._cached_price > 0 and (now - self._cached_price_ts) < 2.5:
            return self._cached_price

        try:
            # Query linear perpetual for live BTC reference price
            url = f"{self.base_url}/v5/market/tickers?category=linear&symbol=BTCUSDT"
            res = self.session.get(url, timeout=3).json()
            if res.get("retCode") == 0 and res.get("result", {}).get("list"):
                ticker = res["result"]["list"][0]
                price = float(ticker.get("lastPrice") or ticker.get("indexPrice") or ticker.get("markPrice"))
                if price > 0:
                    self._cached_price = price
                    self._cached_price_ts = now
                    return price
        except Exception as e:
            print(f"[StrikeSelector] Failed to fetch linear BTCUSDT price: {e}")

        # Fallback to option tickers
        try:
            url = f"{self.base_url}/v5/market/tickers?category=option&baseCoin=BTC"
            res = self.session.get(url, timeout=3).json()
            if res.get("retCode") == 0 and res.get("result", {}).get("list"):
                for t in res["result"]["list"]:
                    ref = float(t.get("underlyingPrice") or t.get("indexPrice") or 0)
                    if ref > 0:
                        self._cached_price = ref
                        self._cached_price_ts = now
                        return ref
        except Exception as e:
            print(f"[StrikeSelector] Failed to fetch option tickers: {e}")

        # If offline or rate limited, return fallback
        return self._cached_price if self._cached_price > 0 else 75000.0

    def get_all_option_contracts(self) -> List[OptionContractInfo]:
        """
        Fetch all active BTC option contracts from Bybit instruments-info endpoint.
        Uses in-memory caching to prevent API rate limits and optimize response times.
        """
        now = time.time()
        if self._cached_contracts and (now - self._cached_contracts_ts) < 45.0:
            return self._cached_contracts

        url = f"{self.base_url}/v5/market/instruments-info?category=option&baseCoin=BTC&limit=1000"
        try:
            res = self.session.get(url, timeout=4).json()
            if res.get("retCode") != 0 or not res.get("result", {}).get("list"):
                if self._cached_contracts:
                    return self._cached_contracts
                raise RuntimeError(f"Bybit API error: {res.get('retMsg', 'Unknown error')}")

            now_ms = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
            contracts: List[OptionContractInfo] = []

            for item in res["result"]["list"]:
                delivery_time_ms = int(item.get("deliveryTime", 0))
                # Ignore expired contracts
                if delivery_time_ms <= now_ms:
                    continue

                raw_symbol = item["symbol"]  # e.g., BTC-7NOV26-75750-C-USDT
                options_type = item.get("optionsType", "").upper()  # "CALL" or "PUT"
                if not options_type:
                    options_type = "CALL" if "-C" in raw_symbol else "PUT"

                # Parse strike price
                parts = raw_symbol.split("-")
                strike = 0.0
                if len(parts) >= 3:
                    try:
                        strike = float(parts[2])
                    except ValueError:
                        strike = float(item.get("strikePrice", 0))
                else:
                    strike = float(item.get("strikePrice", 0))

                if strike <= 0:
                    continue

                instrument_id = f"{raw_symbol}-OPTION.BYBIT"
                exp_dt = datetime.datetime.fromtimestamp(delivery_time_ms / 1000, datetime.timezone.utc)
                expiry_str = exp_dt.strftime("%Y-%m-%d %H:%M UTC")

                min_order_qty = float(item.get("lotSizeFilter", {}).get("minOrderQty", 0.01))
                price_tick_size = float(item.get("priceFilter", {}).get("tickSize", 5.0))

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
        Determines the nearest ATM strike based on BTC reference price and returns
        the matching Call and Put contracts.

        Parameters
        ----------
        target_expiry_ms : int, optional
            The specific expiry timestamp (ms) to target. If None, picks the nearest future expiry.
        target_strike : float, optional
            A manually selected strike price if desired, otherwise defaults to the calculated ATM strike.
        override_btc_price : float, optional
            Use a manual reference price for testing.
        """
        btc_price = override_btc_price if override_btc_price is not None else self.get_btc_reference_price()
        contracts = self.get_all_option_contracts()

        if not contracts:
            raise RuntimeError("No active BTC option contracts found on Bybit.")

        # Identify all unique future expiries
        expiry_map: Dict[int, str] = {}
        for c in contracts:
            if c.delivery_time_ms not in expiry_map:
                expiry_map[c.delivery_time_ms] = c.expiry_str

        sorted_expiries = sorted(expiry_map.keys())
        available_expiries = [{"timestamp": exp, "label": expiry_map[exp]} for exp in sorted_expiries]

        # Select target expiry
        if target_expiry_ms and target_expiry_ms in expiry_map:
            chosen_expiry = target_expiry_ms
        else:
            chosen_expiry = sorted_expiries[0]

        # Filter contracts for chosen expiry
        contracts_at_expiry = [c for c in contracts if c.delivery_time_ms == chosen_expiry]
        strikes = sorted(list(set(c.strike for c in contracts_at_expiry)))

        if not strikes:
            raise RuntimeError(f"No strikes found for expiry {expiry_map[chosen_expiry]}")

        # Calculate nearest ATM strike to BTC reference price
        atm_strike = min(strikes, key=lambda s: abs(s - btc_price))
        chosen_strike = target_strike if (target_strike is not None and target_strike in strikes) else atm_strike

        # Find matching Call and Put for this strike
        call_contract = next(
            (c for c in contracts_at_expiry if c.strike == chosen_strike and c.option_type == "CALL"),
            None,
        )
        put_contract = next(
            (c for c in contracts_at_expiry if c.strike == chosen_strike and c.option_type == "PUT"),
            None,
        )

        if not call_contract or not put_contract:
            raise RuntimeError(
                f"Could not find matching Call and Put for strike {chosen_strike} at expiry {expiry_map[chosen_expiry]}"
            )

        # Build detailed strike matrix for ATM visualization
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
        )

