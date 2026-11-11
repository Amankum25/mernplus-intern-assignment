"""
FastAPI server that powers the web dashboard.
Exposes REST endpoints to control the strategy and a WebSocket endpoint
to stream live events to the browser in real time.
"""

import asyncio
import datetime
from decimal import Decimal
import json
import os
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

from straddle.execution_manager import StraddleExecutionManager
from straddle.persistence import state_persistence
from straddle.strike_selector import BybitStrikeSelector


app = FastAPI(title="NautilusTrader BTC Options Straddle Dashboard")

# allow the dashboard (served from any origin) to call these endpoints
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# one shared manager instance — defaults to DEMO environment
default_env = os.getenv("BYBIT_ENVIRONMENT", "DEMO")
manager = StraddleExecutionManager(environment=default_env)

# all currently connected browser WebSocket clients
connected_websockets: List[WebSocket] = []


def on_telemetry_event(event: Dict[str, Any]) -> None:
    """
    Called by the strategy/execution manager whenever something happens
    (order fill, valuation update, SL/TP trigger, etc.).
    Pushes the event as JSON to every connected browser tab.
    Background threads can't await coroutines directly, so we use run_coroutine_threadsafe.
    """
    if connected_websockets:
        message = json.dumps(event, default=str)
        for ws in connected_websockets:
            try:
                asyncio.run_coroutine_threadsafe(ws.send_text(message), event_loop)
            except Exception:
                pass


manager.register_listener(on_telemetry_event)


# -----------------------------------------------------------------------------
# Request / Response Schemas
# -----------------------------------------------------------------------------

class ConfigRequest(BaseModel):
    """Payload for updating Bybit API credentials or switching environments."""
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    environment: str = "DEMO"  # DEMO or TESTNET


class StartStraddleRequest(BaseModel):
    """Payload to launch a straddle or add size to an existing one."""
    level: int = 2            # 1 = naked, 2 = automated SL/TP
    trade_size: float = 0.01  # BTC per leg
    simulated: bool = False   # True = emulate fills using live quotes, no real orders
    target_expiry_ms: Optional[int] = None
    target_strike: Optional[float] = None


class ShockRequest(BaseModel):
    """Payload for triggering a demo price shock to test SL or TP."""
    shock_type: str  # "STOP_LOSS" or "TAKE_PROFIT"


class AutoExitRequest(BaseModel):
    """Toggle between live market auto-exit and demo hold mode."""
    enabled: bool


# -------------------------------------------------------------------------
# REST Endpoints
# -------------------------------------------------------------------------

@app.get("/api/status")
async def get_status():
    """Engine health, Bybit connection status, and current strategy state."""
    strategy_state = "IDLE"
    if manager.active_strategy:
        st = manager.active_strategy.strategy_state
        strategy_state = st.value if hasattr(st, "value") else str(st)
    elif manager.is_running:
        strategy_state = "OPEN"

    # check if our API key actually works
    bybit_auth = False
    bybit_conn = False
    if manager.bybit_client:
        try:
            res = manager.bybit_client.get_wallet_balance()
            if res.get("retCode") == 0:
                bybit_auth = True
                bybit_conn = True
        except Exception:
            pass

    return {
        "status": "RUNNING" if manager.is_running else "IDLE",
        "strategy_state": strategy_state,
        "exchange": "Bybit",
        "environment": "Testnet",
        "bybit_connected": bybit_conn,
        "market_data_connected": True,
        "private_api_authenticated": bybit_auth,
        "nautilus_running": True,
        "is_simulated": manager.is_simulated,
        "has_keys": bool(manager.api_key and manager.api_secret),
        "latest_telemetry": manager.latest_telemetry,
    }


@app.post("/api/connection/test")
async def test_connection():
    """Tests Bybit API key authentication by calling the wallet balance endpoint."""
    auth_ok = False
    details = "Public market data is active."
    if manager.bybit_client:
        try:
            res = manager.bybit_client.get_wallet_balance()
            if res.get("retCode") == 0:
                auth_ok = True
                details = "Bybit Testnet API authenticated successfully."
            else:
                details = f"Auth failed: {res.get('retMsg', 'Unknown error')}"
        except Exception as e:
            details = f"Connection error: {str(e)}"

    return {
        "bybit_connected": True,
        "market_data_connected": True,
        "private_api_authenticated": auth_ok,
        "nautilus_running": True,
        "message": details,
    }


@app.get("/api/session")
def get_session():
    """
    Returns the full persistent session state: active trade, orders, logs, history, and stats.
    Also auto-reconciles open Bybit positions if the strategy isn't currently tracking them.
    """
    state = state_persistence.get_full_state()
    strategy_state = "IDLE"
    if manager.active_strategy:
        st = manager.active_strategy.strategy_state
        strategy_state = st.value if hasattr(st, "value") else str(st)
    elif manager.is_running:
        strategy_state = "OPEN"

    live_positions = []
    if manager.bybit_client and not manager.is_simulated:
        try:
            p_res = manager.bybit_client.get_open_positions("BTC")
            live_positions = p_res.get("result", {}).get("list", [])
        except Exception:
            pass

    # if Bybit shows open positions but we're not tracking them (e.g. server restarted), reconcile
    if live_positions and not manager.is_running:
        try:
            reconciled = manager.reconcile_positions()
            if reconciled:
                state = state_persistence.get_full_state()
                strategy_state = "OPEN"
        except Exception:
            pass

    # if strategy is idle and there are no live positions, don't show a stale active trade
    if strategy_state in ["IDLE", "CLOSED"] and not live_positions:
        state["active_trade"] = None

    state["live_positions"] = live_positions
    state["is_running"] = manager.is_running
    state["strategy_state"] = strategy_state
    state["environment"] = "Testnet"
    state["auto_exit_on_market"] = manager.auto_exit_on_market
    return state


@app.post("/api/logs/clear")
def clear_logs():
    """Clears the in-memory and on-disk log history."""
    with state_persistence.lock:
        state_persistence.state["logs"] = []
        state_persistence._save_to_disk(state_persistence.state)
    return {"status": "CLEARED"}


@app.get("/api/account")
def get_account():
    """Returns Bybit account balance and margin info."""
    if manager.bybit_client:
        try:
            res = manager.bybit_client.get_wallet_balance()
            if res.get("retCode") == 0:
                list_data = res.get("result", {}).get("list", [])
                if list_data:
                    acct = list_data[0]
                    return {
                        "status": "CONNECTED",
                        "totalEquity": acct.get("totalEquity", "0.0"),
                        "totalWalletBalance": acct.get("totalWalletBalance", "0.0"),
                        "totalMarginBalance": acct.get("totalMarginBalance", "0.0"),
                        "totalAvailableBalance": acct.get("totalAvailableBalance", "0.0"),
                    }
            return {"status": "ERROR", "msg": res.get("retMsg")}
        except Exception as e:
            return {"status": "ERROR", "msg": str(e)}
    return {"status": "NO_KEYS"}


@app.get("/api/market")
def get_market(expiry_ms: Optional[int] = None, strike: Optional[float] = None):
    """Returns live BTC price, available strikes, and ATM straddle quotes."""
    try:
        overview = manager.get_market_overview(target_expiry_ms=expiry_ms, target_strike=strike)
        return overview
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/config")
def update_config(req: ConfigRequest):
    """Swaps in new API credentials or changes environment. Stops any running strategy first."""
    global manager
    was_running = manager.is_running
    if was_running:
        manager.stop_straddle()

    manager = StraddleExecutionManager(
        api_key=req.api_key,
        api_secret=req.api_secret,
        environment=req.environment,
    )
    manager.register_listener(on_telemetry_event)
    return {"status": "SUCCESS", "environment": manager.environment_str}


@app.post("/api/straddle/start")
def start_straddle(req: StartStraddleRequest):
    """Starts the BTC ATM Short Straddle. If one is already running, adds to the position instead."""
    try:
        res = manager.start_straddle(
            level=req.level,
            trade_size=req.trade_size,
            simulated=req.simulated,
            target_expiry_ms=req.target_expiry_ms,
            target_strike=req.target_strike,
        )
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/straddle/scale-in")
def scale_in_straddle(req: StartStraddleRequest):
    """Adds more size to the active straddle position."""
    try:
        return manager.scale_in_straddle(additional_size=req.trade_size)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/straddle/stop")
def stop_straddle():
    """Stops the strategy and closes all open positions."""
    return manager.stop_straddle()


@app.get("/api/orders")
def get_orders():
    """
    Returns the comprehensive order list.
    When running live/demo, fetches actual executed and historical option orders from Bybit V5
    and combines them with local audit logs, ensuring no orders are omitted.
    """
    orders_map: Dict[str, Dict[str, Any]] = {}

    # 1. Populate from persistent local storage
    state = state_persistence.get_full_state()
    for o in state.get("orders", []):
        key = o.get("order_id") or o.get("client_order_id")
        if key:
            orders_map[str(key)] = dict(o)

    # 2. Fetch live Bybit exchange orders (both historical and realtime)
    if manager.bybit_client and not manager.is_simulated:
        try:
            # Query Bybit V5 Option Order History
            hist_res = manager.bybit_client.get_order_history(base_coin="BTC", limit=50)
            if hist_res.get("retCode") == 0:
                bybit_list = hist_res.get("result", {}).get("list", [])
                for b_ord in bybit_list:
                    oid = b_ord.get("orderId")
                    link_id = b_ord.get("orderLinkId")
                    created_ms = int(b_ord.get("createdTime", 0))
                    if created_ms:
                        time_str = datetime.datetime.fromtimestamp(
                            created_ms / 1000.0, tz=datetime.timezone.utc
                        ).strftime("%H:%M:%S")
                    else:
                        time_str = "--"

                    avg_px = float(b_ord.get("avgPrice") or b_ord.get("price", 0.0))
                    qty = float(b_ord.get("qty", 0.0))
                    filled_qty = float(b_ord.get("cumExecQty", 0.0))
                    status = b_ord.get("orderStatus", "NEW").upper()

                    bybit_entry = {
                        "order_id": oid,
                        "order_link_id": link_id,
                        "time": time_str,
                        "contract": b_ord.get("symbol"),
                        "side": b_ord.get("side", "").upper(),
                        "qty": qty,
                        "filled_qty": filled_qty,
                        "avg_price": avg_px,
                        "status": status,
                        "created_ms": created_ms,
                        "source": "BYBIT",
                    }

                    # If this Bybit order corresponds to a local client order ID, merge and remove duplicate
                    if link_id and link_id in orders_map:
                        orders_map.pop(link_id, None)

                    orders_map[oid] = bybit_entry
        except Exception:
            pass

    # Sort newest to oldest so operator sees latest orders first
    def _order_sort_key(o: Dict[str, Any]) -> float:
        if o.get("created_ms"):
            return float(o["created_ms"]) / 1000.0
        t_str = str(o.get("time") or o.get("updated_at") or "")
        try:
            parts = t_str.split(":")
            if len(parts) == 3:
                return float(parts[0]) * 3600.0 + float(parts[1]) * 60.0 + float(parts[2])
        except Exception:
            pass
        return 0.0

    order_list = list(orders_map.values())
    order_list.sort(key=_order_sort_key, reverse=True)
    return order_list


@app.get("/api/positions")
def get_positions():
    """
    Returns open positions from Bybit (live) or from the tracked active trade (simulated).
    If we have API keys, we always prefer the live Bybit source.
    """
    if manager.bybit_client and not manager.is_simulated:
        try:
            res = manager.bybit_client.get_open_positions("BTC")
            positions = res.get("result", {}).get("list", [])
            if positions:
                return positions
        except Exception:
            pass

    # fallback: reconstruct position list from the active trade in persistent state
    st_val = ""
    if manager.active_strategy:
        st = manager.active_strategy.strategy_state
        st_val = st.value if hasattr(st, "value") else str(st)
    if manager.is_running or (st_val in ["OPEN", "ACTIVE_POSITION", "PARTIALLY_FILLED"]):
        trade = state_persistence.state.get("active_trade")
        if trade and trade.get("is_active"):
            return [
                {
                    "symbol": trade.get("call_symbol"),
                    "side": "Sell",
                    "size": str(trade.get("trade_size", 0.01)),
                    "avgPrice": str(trade.get("call_entry_price", 0.0)),
                    "markPrice": str(trade.get("call_ask", 0.0)),
                    "unrealisedPnl": str(round((trade.get("call_entry_price", 0.0) - trade.get("call_ask", 0.0)) * trade.get("trade_size", 0.01), 4)),
                },
                {
                    "symbol": trade.get("put_symbol"),
                    "side": "Sell",
                    "size": str(trade.get("trade_size", 0.01)),
                    "avgPrice": str(trade.get("put_entry_price", 0.0)),
                    "markPrice": str(trade.get("put_ask", 0.0)),
                    "unrealisedPnl": str(round((trade.get("put_entry_price", 0.0) - trade.get("put_ask", 0.0)) * trade.get("trade_size", 0.01), 4)),
                }
            ]
    return []


@app.post("/api/straddle/shock")
def simulate_shock(req: ShockRequest):
    """
    Demo helper: injects a synthetic price move to trigger either the 1% SL or 2% TP.
    Useful for showing the automated exit during an interview or demo.
    """
    try:
        return manager.simulate_price_shock(req.shock_type)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/straddle/auto-exit-mode")
def set_auto_exit_mode(req: AutoExitRequest):
    """Toggles whether the strategy auto-exits on live market moves vs only on demand."""
    manager.auto_exit_on_market = req.enabled
    return {"status": "SUCCESS", "auto_exit_on_market": manager.auto_exit_on_market}


@app.get("/api/walkthrough")
async def get_walkthrough():
    """Returns the architectural Q&A for the 10 interview questions about the system."""
    return {
        "questions": [
            {
                "id": 1,
                "question": "How does NautilusTrader start and initialize?",
                "summary": "NautilusTrader uses an asynchronous event-driven TradingNode architecture.",
                "details": (
                    "When a TradingNode starts, it initializes core subsystems: the MessageBus (high-speed event bus), "
                    "LiveDataEngine (market data feed manager), LiveExecEngine (order lifecycle & reconciliation), "
                    "LiveRiskEngine (pre-trade risk checks), Portfolio (position and cash tracking), and Cache "
                    "(instrument & order book repository). It registers client factories (e.g. BybitLiveDataClientFactory "
                    "and BybitLiveExecClientFactory), establishes WebSocket/REST gateways, loads instrument definitions, "
                    "attaches user Strategy instances, and starts the event loop."
                ),
            },
            {
                "id": 2,
                "question": "How have you configured the Bybit integration?",
                "summary": "Configured via BybitDataClientConfig and BybitExecClientConfig targeting Bybit DEMO / TESTNET.",
                "details": (
                    "We instantiate BybitDataClientConfig and BybitExecClientConfig with BybitEnvironment.DEMO "
                    "(or TESTNET), specifying product_types=(BybitProductType.OPTION,). For execution, Bybit utilizes "
                    "unified margin accounts (UTA). The data client connects to Bybit's public WebSocket stream "
                    "(wss://stream.bybit.com/v5/public/option) for ticker quotes and Greeks, while the execution client "
                    "connects to private stream (wss://stream-demo.bybit.com/v5/private) and trade stream "
                    "(wss://stream-demo.bybit.com/v5/trade) with API key and HMAC-SHA256 signature authentication."
                ),
            },
            {
                "id": 3,
                "question": "How does NautilusTrader receive market data?",
                "summary": "Rust-native WebSocket client streaming into Python via zero-copy PyCapsules.",
                "details": (
                    "Bybit publishes ticker and order book messages over WebSocket in JSON. NautilusTrader's "
                    "high-performance Rust core (nautilus_pyo3) parses incoming binary/JSON frames into memory buffers "
                    "and wraps them into zero-copy PyCapsules. The BybitDataClient receives capsules, converts them into "
                    "domain objects (QuoteTick, TradeTick, OptionGreeks), and publishes them onto the Nautilus MessageBus, "
                    "which invokes strategy callbacks like on_quote_tick() in microseconds."
                ),
            },
            {
                "id": 4,
                "question": "How are BTC option instruments identified and subscribed to?",
                "summary": "BybitStrikeSelector queries Bybit v5 instruments-info and determines the nearest ATM strike.",
                "details": (
                    "1. The BybitStrikeSelector queries the Bybit linear perpetual ticker (BTCUSDT) to get the underlying "
                    "BTC reference price.\n"
                    "2. It fetches all active BTC option contracts, filters out expired series, and sorts by expiration.\n"
                    "3. For the nearest expiration date, it scans available strike prices and selects the ATM strike: "
                    "min(strikes, key=lambda s: abs(s - btc_price)).\n"
                    "4. It selects the exact matching Call (BTC-DDMMMYY-STRIKE-C-USDT-OPTION.BYBIT) and Put "
                    "(BTC-DDMMMYY-STRIKE-P-USDT-OPTION.BYBIT).\n"
                    "5. The strategy calls self.subscribe_quote_ticks(call_id) and self.subscribe_quote_ticks(put_id)."
                ),
            },
            {
                "id": 5,
                "question": "How does the strategy receive market data events?",
                "summary": "Through type-safe callbacks: on_quote_tick, on_trade_tick, and on_option_greeks.",
                "details": (
                    "When market data arrives on the MessageBus matching a subscribed InstrumentId, the Strategy router "
                    "dispatches the event to the appropriate handler: on_quote_tick(tick: QuoteTick) delivers top-of-book "
                    "bid/ask prices; on_trade_tick(tick: TradeTick) delivers trades; on_option_greeks delivers delta, "
                    "gamma, vega, and IV. In our straddle, on_quote_tick() continuously updates the current ask price for "
                    "each leg to value the combined buyback premium."
                ),
            },
            {
                "id": 6,
                "question": "How are orders submitted to Bybit?",
                "summary": "Via self.order_factory.market() or limit() submitted to ExecutionEngine.",
                "details": (
                    "The strategy uses self.order_factory.market(instrument_id=..., order_side=OrderSide.SELL, quantity=qty) "
                    "to construct a MarketOrder domain object stamped with a unique ClientOrderId. Calling "
                    "self.submit_order(order) generates a SubmitOrder command sent over the internal bus to the "
                    "LiveExecEngine. The BybitExecutionClient formats this into a Bybit v5 REST (/v5/order/create) or "
                    "WebSocket trade request."
                ),
            },
            {
                "id": 7,
                "question": "How does the system know whether an order was accepted or executed?",
                "summary": "Through asynchronous event callbacks: on_order_accepted and on_order_filled.",
                "details": (
                    "Order states transition deterministically:\n"
                    "- SUBMITTED: Local strategy generated and dispatched order (on_order_submitted).\n"
                    "- ACCEPTED: Bybit venue acknowledged receipt, validated margin, and assigned a VenueOrderId (on_order_accepted).\n"
                    "- REJECTED: Venue rejected order due to margin or lot size error (on_order_rejected).\n"
                    "- FILLED: Execution match report received from Bybit private WebSocket with execution price and filled quantity (on_order_filled)."
                ),
            },
            {
                "id": 8,
                "question": "How are positions tracked?",
                "summary": "By NautilusTrader's Portfolio subsystem and position lifecycle callbacks.",
                "details": (
                    "When an OrderFilled event occurs, the Portfolio updates net signed quantity and average entry price. "
                    "It emits PositionOpened, PositionChanged, and PositionClosed events. The strategy tracks active positions "
                    "via self.portfolio.position(instrument_id), inspecting position.signed_qty, position.avg_px_open, "
                    "and position.unrealized_pnl()."
                ),
            },
            {
                "id": 9,
                "question": "How have you implemented the Stop-Loss and Take-Profit?",
                "summary": "Continuous ask-side buyback valuation against combined entry premium with idempotency guards.",
                "details": (
                    "When entering the short straddle, both legs fill at sell prices:\n"
                    "  Combined Entry Premium = Call Entry Price + Put Entry Price\n"
                    "While holding the positions, on every quote tick:\n"
                    "  Combined Buyback Premium = Call Ask Price + Put Ask Price\n"
                    "- Stop-Loss (1%): If Buyback Premium >= Entry Premium * 1.01 (+1% cost increase), trigger exit.\n"
                    "- Take-Profit (2%): If Buyback Premium <= Entry Premium * 0.98 (-2% cost decrease), trigger exit.\n"
                    "When triggered, self.exit_triggered = True guards against duplicate executions. The strategy submits "
                    "closing BUY orders (reduce_only=True) to flatten both short positions."
                ),
            },
            {
                "id": 10,
                "question": "What happens when one leg gets executed but the other leg does not?",
                "summary": "Legging Risk Protocol: Safety timeout, cancel unfilled order, and immediate unwind of open leg.",
                "details": (
                    "Entering two separate option legs creates 'legging risk'. If Leg A fills but Leg B is rejected or "
                    "times out unfilled:\n"
                    "1. The strategy detects an unhedged state via a fill timeout timer or on_order_rejected callback.\n"
                    "2. It immediately cancels any outstanding Leg B order via self.cancel_order().\n"
                    "3. It immediately closes/buys back the filled Leg A position using a market order, preventing "
                    "unhedged directional delta/gamma exposure from blowing up the portfolio.\n"
                    "4. If a partial fill occurs, it balances contract quantities or reduces to the minimum matched size."
                ),
            },
        ]
    }


# -------------------------------------------------------------------------
# Dashboard HTML
# -------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def get_index():
    """Serves the main dashboard HTML page."""
    template_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    if os.path.exists(template_path):
        with open(template_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h3>Dashboard template loading...</h3>")


# -------------------------------------------------------------------------
# WebSocket Endpoint
# -------------------------------------------------------------------------

@app.websocket("/ws/stream")
async def websocket_endpoint(websocket: WebSocket):
    """
    Real-time event stream for the dashboard.
    On connect, sends the current state snapshot so the UI initializes immediately.
    Then keeps the connection open and receives keepalive pings.
    """
    await websocket.accept()
    connected_websockets.append(websocket)
    try:
        # send current snapshot immediately so the UI doesn't start blank
        await websocket.send_text(json.dumps(manager.latest_telemetry, default=str))
        while True:
            await websocket.receive_text()  # keepalive ping from client
    except WebSocketDisconnect:
        if websocket in connected_websockets:
            connected_websockets.remove(websocket)
    except Exception:
        if websocket in connected_websockets:
            connected_websockets.remove(websocket)


# background threads need this to push events to the asyncio WebSocket loop
event_loop = asyncio.get_event_loop()
