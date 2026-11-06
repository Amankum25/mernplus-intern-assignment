"""
Direct authenticated Bybit V5 REST client for Demo Trading and Testnet.
Handles order submission, cancellation, position tracking, and wallet balance querying.
"""

import hashlib
import hmac
import json
import time
from typing import Any, Dict, List, Optional
import requests


class BybitV5Client:
    """
    Authenticated Bybit V5 REST Client for Options Trading on Demo / Testnet.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://api-testnet.bybit.com",
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.time_offset_ms: int = 0
        self.session = requests.Session()
        self._cached_positions: Dict[str, Any] = {}
        self._cached_positions_ts: float = 0.0
        self._cached_wallet: Dict[str, Any] = {}
        self._cached_wallet_ts: float = 0.0
        self._sync_time()

    def _sync_time(self) -> None:
        """Synchronize clock with Bybit server time."""
        try:
            local_ms = int(time.time() * 1000)
            res = self.session.get(f"{self.base_url}/v5/market/time", timeout=3).json()
            if res.get("retCode") == 0:
                server_ms = int(res["result"]["timeNano"]) // 1000000
                self.time_offset_ms = server_ms - local_ms
        except Exception:
            self.time_offset_ms = 0

    def _generate_signature(self, timestamp: int, payload_str: str, recv_window: int = 10000) -> str:
        param_str = str(timestamp) + self.api_key + str(recv_window) + payload_str
        return hmac.new(
            self.api_secret.encode("utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _headers(self, payload_str: str = "", recv_window: int = 10000) -> Dict[str, str]:
        timestamp = int(time.time() * 1000) + self.time_offset_ms
        signature = self._generate_signature(timestamp, payload_str, recv_window)
        return {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": str(recv_window),
            "Content-Type": "application/json",
        }

    def get_wallet_balance(self, account_type: str = "UNIFIED") -> Dict[str, Any]:
        """Fetch wallet balance and margin status from Bybit."""
        now = time.time()
        if self._cached_wallet and (now - self._cached_wallet_ts) < 2.5:
            return self._cached_wallet

        endpoint = f"/v5/account/wallet-balance?accountType={account_type}"
        timestamp = int(time.time() * 1000) + self.time_offset_ms
        param_str = str(timestamp) + self.api_key + "10000" + f"accountType={account_type}"
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": "10000",
        }
        try:
            res = self.session.get(f"{self.base_url}{endpoint}", headers=headers, timeout=6).json()
            if res.get("retCode") == 0:
                self._cached_wallet = res
                self._cached_wallet_ts = now
            return res
        except Exception as e:
            return self._cached_wallet if self._cached_wallet else {"retCode": -1, "retMsg": str(e)}

    def place_order(
        self,
        symbol: str,
        side: str,  # "Buy" or "Sell"
        order_type: str = "Market",
        qty: str = "0.01",
        price: Optional[str] = None,
        reduce_only: bool = False,
        order_filter: str = "Order",
        order_link_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Submit an order for a BTC option contract on Bybit V5.
        """
        self._cached_positions_ts = 0.0  # Invalidate position cache immediately
        endpoint = "/v5/order/create"
        client_oid = order_link_id or f"{symbol[:6]}-{side.upper()}-{int(time.time()*1000)}"
        tif = "IOC" if order_type.lower() == "market" else "GTC"
        payload = {
            "category": "option",
            "symbol": symbol,
            "side": side.capitalize(),
            "orderType": order_type.capitalize(),
            "qty": str(qty),
            "timeInForce": tif,
            "orderFilter": order_filter,
            "orderLinkId": client_oid,
        }
        if reduce_only:
            payload["reduceOnly"] = True
        if price is not None and order_type.lower() == "limit":
            payload["price"] = str(price)

        payload_str = json.dumps(payload)
        headers = self._headers(payload_str, recv_window=10000)
        res = self.session.post(f"{self.base_url}{endpoint}", data=payload_str, headers=headers, timeout=8).json()
        return res

    def cancel_order(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """Cancel an open order."""
        self._cached_positions_ts = 0.0
        endpoint = "/v5/order/cancel"
        payload = {
            "category": "option",
            "symbol": symbol,
            "orderId": order_id,
        }
        payload_str = json.dumps(payload)
        headers = self._headers(payload_str, recv_window=10000)
        res = self.session.post(f"{self.base_url}{endpoint}", data=payload_str, headers=headers, timeout=8).json()
        return res

    def get_open_positions(self, base_coin: str = "BTC") -> Dict[str, Any]:
        """Get open BTC option positions with short in-memory cache to prevent redundant round-trips."""
        now = time.time()
        if self._cached_positions and (now - self._cached_positions_ts) < 1.2:
            return self._cached_positions

        query_str = f"baseCoin={base_coin}&category=option"
        endpoint = f"/v5/position/list?{query_str}"
        timestamp = int(time.time() * 1000) + self.time_offset_ms
        param_str = str(timestamp) + self.api_key + "10000" + query_str
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": "10000",
        }
        try:
            res = self.session.get(f"{self.base_url}{endpoint}", headers=headers, timeout=6).json()
            if res.get("retCode") == 0:
                self._cached_positions = res
                self._cached_positions_ts = now
            return res
        except Exception as e:
            return self._cached_positions if self._cached_positions else {"retCode": -1, "retMsg": str(e), "result": {"list": []}}

    def apply_demo_funds(self, coin: str = "USDT", amount: str = "100000") -> Dict[str, Any]:
        """
        Request simulated practice funds on Bybit Demo Trading.
        Supported coins: USDT (up to 100,000), BTC (up to 15), ETH (up to 200).
        """
        endpoint = "/v5/account/demo-apply-money"
        payload = {
            "adjustType": 0,
            "utaDemoApplyMoney": [
                {
                    "coin": coin,
                    "amountStr": str(amount),
                }
            ],
        }
        payload_str = json.dumps(payload)
        headers = self._headers(payload_str, recv_window=10000)
        res = requests.post(f"{self.base_url}{endpoint}", data=payload_str, headers=headers, timeout=10).json()
        return res

