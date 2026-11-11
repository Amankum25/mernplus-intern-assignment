"""
Direct authenticated Bybit V5 REST client.
Handles order placement, cancellation, position queries, and wallet balance checks.
Used for startup reconciliation, live order execution, and dashboard status.
"""

import hashlib
import hmac
import json
import time
from typing import Any, Dict, List, Optional
import requests


class BybitV5Client:
    """
    Wraps Bybit's V5 REST API with HMAC-SHA256 authentication.
    Every private endpoint (orders, positions, wallet) requires a signed request.
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
        self.session = requests.Session()  # reuse TCP connection across requests instead of opening a new one each time
        self._cached_positions: Dict[str, Any] = {}
        self._cached_positions_ts: float = 0.0
        self._cached_wallet: Dict[str, Any] = {}
        self._cached_wallet_ts: float = 0.0

        # Synchronize local clock with Bybit server time, factoring in round-trip latency
        self._sync_time()

    def _sync_time(self) -> None:
        """
        Synchronizes local clock with Bybit server time, accounting for network latency.
        We subtract a 500ms safety buffer so our timestamp is NEVER in the future.
        (Bybit immediately rejects any request where timestamp is > 1000ms ahead of server time).
        """
        try:
            t0 = int(time.time() * 1000)
            res = self.session.get(f"{self.base_url}/v5/market/time", timeout=4).json()
            t1 = int(time.time() * 1000)
            if res.get("retCode") == 0:
                server_ms = int(res["result"]["timeNano"]) // 1_000_000
                rtt = max(0, t1 - t0)
                # server_ms was sampled approximately at t0 + (rtt / 2)
                self.time_offset_ms = server_ms - (t0 + rtt // 2) - 500
        except Exception:
            self.time_offset_ms = -500

    def _generate_signature(self, timestamp: int, payload_str: str, recv_window: int = 20000) -> str:
        """
        Bybit requires every private request to be signed with our API secret.
        The signature formula is: HMAC_SHA256(secret, timestamp + api_key + recv_window + payload)
        """
        param_str = str(timestamp) + self.api_key + str(recv_window) + payload_str
        return hmac.new(
            self.api_secret.encode("utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _headers(self, payload_str: str = "", recv_window: int = 20000) -> Dict[str, str]:
        """
        Builds the auth headers that every private Bybit request needs.
        The timestamp is adjusted by our clock offset so it matches Bybit's server time.
        """
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
        """
        Returns available margin from the Bybit Unified Trading Account.
        Cached for 2.5 seconds so the dashboard can poll without hitting rate limits.
        """
        now = time.time()
        if self._cached_wallet and (now - self._cached_wallet_ts) < 2.5:
            return self._cached_wallet

        endpoint = f"/v5/account/wallet-balance?accountType={account_type}"
        timestamp = int(time.time() * 1000) + self.time_offset_ms
        recv_window = 20000
        param_str = str(timestamp) + self.api_key + str(recv_window) + f"accountType={account_type}"
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": str(recv_window),
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
        reduce_only: bool = False,  # True = only close existing position, never open a new one
        order_filter: Optional[str] = None,
        order_link_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Sends a buy or sell order to Bybit for a BTC option contract.
        We invalidate the position cache immediately so the next query reflects the new order.
        """
        self._cached_positions_ts = 0.0
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
            "orderLinkId": client_oid,
        }
        # Note: orderFilter is valid only for Spot trading in Bybit V5; omitted for options
        if order_filter and order_filter != "Order":
            payload["orderFilter"] = order_filter
        if reduce_only:
            payload["reduceOnly"] = True
        if price is not None and order_type.lower() == "limit":
            payload["price"] = str(price)

        payload_str = json.dumps(payload)
        headers = self._headers(payload_str, recv_window=20000)
        res = self.session.post(f"{self.base_url}{endpoint}", data=payload_str, headers=headers, timeout=8).json()

        # Automatic time recovery: if clock skew error occurred, resync time and retry once
        if res.get("retCode") == 10002:
            self._sync_time()
            headers = self._headers(payload_str, recv_window=20000)
            res = self.session.post(f"{self.base_url}{endpoint}", data=payload_str, headers=headers, timeout=8).json()

        return res

    def cancel_order(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """
        Cancels an open order on Bybit.
        Called during legging failure — if one leg fills and the other times out,
        we cancel the pending order before closing the filled leg.
        """
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
        """
        Fetches all currently open BTC option positions from Bybit.
        Cached for 1.2 seconds so the dashboard can poll every second without being rate-limited.
        """
        now = time.time()
        if self._cached_positions and (now - self._cached_positions_ts) < 1.2:
            return self._cached_positions

        query_str = f"baseCoin={base_coin}&category=option"
        endpoint = f"/v5/position/list?{query_str}"
        timestamp = int(time.time() * 1000) + self.time_offset_ms
        param_str = str(timestamp) + self.api_key + "20000" + query_str
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": "20000",
        }
        try:
            res = self.session.get(f"{self.base_url}{endpoint}", headers=headers, timeout=6).json()
            if res.get("retCode") == 0:
                self._cached_positions = res
                self._cached_positions_ts = now
            return res
        except Exception as e:
            return self._cached_positions if self._cached_positions else {"retCode": -1, "retMsg": str(e), "result": {"list": []}}

    def get_order_history(self, base_coin: str = "BTC", limit: int = 50) -> Dict[str, Any]:
        """
        Fetches historical option orders from Bybit V5:
        GET /v5/order/history?category=option&baseCoin=BTC&limit=50
        """
        query_str = f"baseCoin={base_coin}&category=option&limit={limit}"
        endpoint = f"/v5/order/history?{query_str}"
        timestamp = int(time.time() * 1000) + self.time_offset_ms
        param_str = str(timestamp) + self.api_key + "20000" + query_str
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": "20000",
        }
        try:
            res = self.session.get(f"{self.base_url}{endpoint}", headers=headers, timeout=8).json()
            return res
        except Exception as e:
            return {"retCode": -1, "retMsg": str(e), "result": {"list": []}}

    def get_open_orders(self, base_coin: str = "BTC") -> Dict[str, Any]:
        """
        Fetches currently active / unfilled option orders from Bybit V5:
        GET /v5/order/realtime?category=option&baseCoin=BTC
        """
        query_str = f"baseCoin={base_coin}&category=option"
        endpoint = f"/v5/order/realtime?{query_str}"
        timestamp = int(time.time() * 1000) + self.time_offset_ms
        param_str = str(timestamp) + self.api_key + "20000" + query_str
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": "20000",
        }
        try:
            res = self.session.get(f"{self.base_url}{endpoint}", headers=headers, timeout=8).json()
            return res
        except Exception as e:
            return {"retCode": -1, "retMsg": str(e), "result": {"list": []}}

    def apply_demo_funds(self, coin: str = "USDT", amount: str = "100000") -> Dict[str, Any]:
        """
        Requests simulated practice funds on Bybit Demo Trading.
        Only works on the demo environment — not on testnet or mainnet.
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
