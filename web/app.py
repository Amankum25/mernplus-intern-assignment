"""
================================================================================
⚡ FASTAPI REAL-TIME DASHBOARD SERVER (web/app.py)
================================================================================
Powers the interactive command terminal and real-time options telemetry feed.

Architecture & Interview Notes:
1. RESTful Control Endpoints:
   - `/api/market`: Fetches live BTC price, available options expiry dates, ATM strike,
     and top-of-book bid/ask quotes via NautilusTrader's Bybit adapter.
   - `/api/straddle/start`: Submits Level 1 (naked) or Level 2 (1% SL / 2% TP) straddle orders.
   - `/api/straddle/stop` & `/api/straddle/close-strategy`: Closes mapped Nautilus position IDs.
   - `/api/orders`: Returns live Nautilus cache orders merged with durable audit logs.
   - `/api/positions`: Exposes 100% verified positions directly from NautilusTrader's live cache.
2. Real-Time Streaming:
   - WebSocket `/ws/stream`: Delivers sub-second event broadcasts (ticks, fills, risk triggers)
     directly to the browser without requiring aggressive client-side polling.
3. Separation of Concerns:
   - Endpoints do NOT connect directly to Bybit; they delegate all trading and data queries
     to `StraddleExecutionManager`, preserving single-point-of-truth and thread safety.
================================================================================
"""

import asyncio
import datetime
from decimal import Decimal
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

logger = logging.getLogger(__name__)

from straddle.execution_manager import StraddleExecutionManager
from straddle.persistence import state_persistence

# Lightweight in-memory session state
session_state: Dict[str, Any] = {
    "active_trade": None,
    "orders": [],
    "logs": [
        {
            "time": datetime.datetime.now().strftime("%H:%M:%S"),
            "category": "ENGINE",
            "message": "Dashboard ready; NautilusTrader connection starts with live strategy initialization.",
            "badge_type": "info",
        },
        {
            "time": datetime.datetime.now().strftime("%H:%M:%S"),
            "category": "DISCOVERY",
            "message": "Market discovery is provided by NautilusTrader's native Bybit adapter.",
            "badge_type": "info",
        },
    ],
}


app = FastAPI(title="NautilusTrader BTC Options Straddle Dashboard")

# allow the dashboard (served from any origin) to call these endpoints
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

default_env = os.getenv("BYBIT_ENVIRONMENT", "DEMO")
manager = StraddleExecutionManager(environment=default_env)

connected_websockets: List[WebSocket] = []
main_event_loop: Optional[asyncio.AbstractEventLoop] = None


def _update_session_from_event(event: Dict[str, Any]) -> None:
    """Updates in-memory session_state (active_trade, orders, logs) from incoming telemetry events."""
    evt_type = event.get("type", "")
    now_str = datetime.datetime.now().strftime("%H:%M:%S")

    if evt_type == "ENTRY_SUBMITTED":
        session_state["logs"].append({
            "time": now_str,
            "category": "SUBMIT",
            "message": f"Submitted straddle orders {event.get('call_order_id')} & {event.get('put_order_id')}",
            "badge_type": "info",
        })

    elif evt_type == "STRADDLE_POSITION_ESTABLISHED":
        combined = event.get("combined_entry", 0.0)
        sl = event.get("sl_target", 0.0)
        tp = event.get("tp_target", 0.0)
        call_px = event.get("call_price", 0.0)
        put_px = event.get("put_price", 0.0)
        qty = event.get("qty", 0.01)

        call_sym = str(event.get("call_symbol") or "CALL")
        put_sym = str(event.get("put_symbol") or "PUT")

        session_state["active_trade"] = {
            "level": manager.active_strategy.config.level if manager.active_strategy else 2,
            "combined_entry_premium": combined,
            "current_buyback_premium": combined,
            "stop_loss_target": sl,
            "take_profit_target": tp,
            "pnl_usd": 0.0,
            "pnl_pct": 0.0,
            "status": "OPEN",
            "call_symbol": call_sym,
            "put_symbol": put_sym,
            "trade_size": qty,
        }

        session_state["logs"].append({
            "time": now_str,
            "category": "FILL",
            "message": f"Straddle established: Combined entry ${combined:.2f} (SL: ${sl:.2f}, TP: ${tp:.2f})",
            "badge_type": "success",
        })

    elif evt_type == "VALUATION_UPDATE":
        if session_state.get("active_trade") and session_state["active_trade"].get("status") == "OPEN":
            session_state["active_trade"]["current_buyback_premium"] = event.get("buyback_premium", 0.0)
            session_state["active_trade"]["pnl_pct"] = event.get("pnl_pct", 0.0)
            session_state["active_trade"]["pnl_usd"] = event.get("pnl_usd", 0.0)

    elif evt_type in ("STOP_LOSS_TRIGGERED", "TAKE_PROFIT_TRIGGERED", "MANUAL_STOP", "STRATEGY_STOPPED"):
        buyback = event.get("buyback_premium", 0.0)
        pnl = event.get("pnl_pct", 0.0)
        # Close state is removed only by confirmed Nautilus position-close callbacks.
        session_state["active_trade"] = state_persistence.get_active_trade()

        badge = "danger" if "STOP_LOSS" in evt_type else ("success" if "TAKE_PROFIT" in evt_type else "info")
        category = "RISK" if "STOP_LOSS" in evt_type else ("PROFIT" if "TAKE_PROFIT" in evt_type else "STOP")
        session_state["logs"].append({
            "time": now_str,
            "category": category,
            "message": f"{evt_type}: Buyback ${buyback:.2f} | Final Return: {pnl:+.2f}%",
            "badge_type": badge,
        })


def on_telemetry_event(event: Dict[str, Any]) -> None:
    """Dispatches telemetry events to local session_state and connected browser tabs."""
    _update_session_from_event(event)

    global main_event_loop
    if connected_websockets:
        if not main_event_loop or not main_event_loop.is_running():
            try:
                main_event_loop = asyncio.get_running_loop()
            except RuntimeError:
                return

        message = json.dumps(event, default=str)
        for ws in list(connected_websockets):
            try:
                asyncio.run_coroutine_threadsafe(ws.send_text(message), main_event_loop)
            except Exception:
                pass


manager.register_listener(on_telemetry_event)


# -----------------------------------------------------------------------------
# Request / Response Schemas
# -----------------------------------------------------------------------------

class ConfigRequest(BaseModel):
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    environment: str = "DEMO"


class StartStraddleRequest(BaseModel):
    level: int = 2
    trade_size: float = 0.01
    simulated: bool = False
    target_expiry_ms: Optional[int] = None
    target_strike: Optional[float] = None


class ShockRequest(BaseModel):
    shock_type: str
    strategy_id: Optional[str] = None
    symbol: Optional[str] = None


class AutoExitRequest(BaseModel):
    enabled: bool


class StopRequest(BaseModel):
    strategy_id: Optional[str] = None
    symbol: Optional[str] = None


# -------------------------------------------------------------------------
# Lifecycle Events
# -------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup():
    global main_event_loop
    main_event_loop = asyncio.get_running_loop()
    manager.selector.set_event_loop(main_event_loop)
    try:
        await asyncio.to_thread(manager.start_for_saved_entries)
        if manager._node_is_running():
            await asyncio.to_thread(manager.reconcile_positions)
    except Exception as e:
        print(f"[Startup] Position reconciliation notice: {e}")


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
    elif manager._node_is_running():
        strategy_state = "OPEN"
    elif manager.is_running:
        strategy_state = "WAITING_FOR_NAUTILUS"

    node_running = manager._node_is_running()
    has_keys = bool(manager.api_key and manager.api_secret)
    return {
        "status": "RUNNING" if node_running else ("DEGRADED" if state_persistence.get_active_strategies() else "IDLE"),
        "strategy_state": strategy_state,
        "exchange": "Bybit",
        "environment": manager.environment_str,
        "bybit_connected": node_running and manager.account_state().get("status") == "CONNECTED",
        "market_data_connected": node_running,
        "private_api_authenticated": node_running and manager.account_state().get("status") == "CONNECTED",
        "nautilus_running": node_running,
        "is_simulated": manager.is_simulated,
        "has_keys": has_keys,
        "latest_telemetry": manager.latest_telemetry,
    }


@app.post("/api/connection/test")
async def test_connection():
    """Tests Bybit API configuration."""
    has_keys = bool(manager.api_key and manager.api_secret)
    node_running = manager._node_is_running()
    return {
        "bybit_connected": node_running and has_keys,
        "market_data_connected": node_running,
        "private_api_authenticated": node_running and has_keys,
        "nautilus_running": node_running,
        "message": "NautilusTrader TradingNode is connected." if node_running else "NautilusTrader TradingNode is not running.",
    }


@app.get("/api/session")
def get_session():
    """Returns the persistent and live session state: active trade, positions, orders, logs, and status."""
    # Reconcile only from NautilusTrader's live cache; never query Bybit from a route.
    now = time.time()
    if manager._node_is_running() and (now - getattr(manager, "_last_exit_ts", 0.0)) > 4.0:
        if (now - getattr(manager, "_last_reconcile_ts", 0.0)) > 1.5:
            manager._last_reconcile_ts = now
            try:
                manager.reconcile_positions()
            except Exception:
                pass

    state = dict(session_state)

    active_strats = state_persistence.get_active_strategies()
    live_active_strats = [
        item for item in active_strats
        if item.get("live_verified", True)
        and item.get("status") != "RECONCILIATION_REQUIRED"
    ]
    node_running = manager._node_is_running()

    strategy_state = "IDLE"
    if manager.active_strategy:
        st = manager.active_strategy.strategy_state
        strategy_state = st.value if hasattr(st, "value") else str(st)
    elif node_running and live_active_strats:
        strategy_state = "OPEN"
    elif live_active_strats:
        strategy_state = "WAITING_FOR_NAUTILUS_SYNC"
    else:
        strategy_state = "IDLE"

    if active_strats:
        session_state["active_trade"] = active_strats[0]
        state["active_trade"] = active_strats[0]
    else:
        session_state["active_trade"] = None
        state["active_trade"] = None

    state["active_strategies"] = active_strats

    state["live_positions"] = manager.live_positions()
    state["is_running"] = node_running
    state["strategy_state"] = strategy_state
    state["environment"] = manager.environment_str
    state["auto_exit_on_market"] = manager.auto_exit_on_market
    state["orders"] = manager.order_history()

    # Include persisted logs
    persisted_logs = state_persistence.get_logs()
    if persisted_logs:
        existing_msgs = {l.get("message") for l in session_state.get("logs", [])}
        for l in persisted_logs:
            if l.get("message") not in existing_msgs:
                session_state["logs"].append(l)
                existing_msgs.add(l.get("message"))
        state["logs"] = session_state["logs"]

    return state


@app.post("/api/logs/clear")
def clear_logs():
    """Clears the in-memory and persistent log history."""
    session_state["logs"].clear()
    state_persistence.clear_logs()
    return {"status": "CLEARED"}


@app.get("/api/account")
def get_account():
    """Returns the balance object currently reconciled into NautilusTrader's account cache."""
    return manager.account_state()


@app.get("/api/market")
def get_market(expiry_ms: Optional[int] = None, strike: Optional[float] = None):
    """Returns live BTC price, available strikes, and ATM straddle quotes."""
    try:
        overview = manager.get_market_overview(target_expiry_ms=expiry_ms, target_strike=strike)
        return overview
    except Exception as e:
        logger.exception("NautilusTrader Bybit market-data request failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/config")
def update_config(req: ConfigRequest):
    """Apply credentials/environment only when no live TradingNode or saved entry can be orphaned."""
    global manager
    if manager._node_is_running() or state_persistence.get_active_strategies():
        raise HTTPException(status_code=409, detail="Stop and reconcile all NautilusTrader positions before changing venue credentials or environment.")

    manager = StraddleExecutionManager(
        api_key=req.api_key,
        api_secret=req.api_secret,
        environment=req.environment,
    )
    if main_event_loop and main_event_loop.is_running():
        manager.selector.set_event_loop(main_event_loop)
    manager.register_listener(on_telemetry_event)
    return {"status": "SUCCESS", "environment": manager.environment_str}


@app.post("/api/straddle/start")
def start_straddle(req: StartStraddleRequest):
    """Starts the BTC ATM Short Straddle."""
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
    """Opens a separate straddle entry for an already-active strike/expiry."""
    try:
        return manager.scale_in_straddle(
            additional_size=req.trade_size,
            target_strike=req.target_strike,
            target_expiry_ms=req.target_expiry_ms,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/straddle/stop")
def stop_straddle(req: Optional[StopRequest] = None):
    """Stops all strategies, or closes both legs of a specific strategy if strategy_id is provided."""
    target_id = (req.strategy_id or req.symbol) if req else None
    if target_id and target_id != "ALL":
        res = manager.close_strategy(target_id)
        return res

    res = manager.stop_straddle()
    return res


@app.post("/api/straddle/close-strategy")
def close_single_strategy(req: StopRequest):
    """Closes BOTH LEGS of the specific straddle strategy."""
    target_id = req.strategy_id or req.symbol
    if not target_id:
        raise HTTPException(status_code=400, detail="Missing strategy_id.")
    res = manager.close_strategy(target_id)
    return res


@app.post("/api/straddle/close-position")
def close_single_position(req: StopRequest):
    """Legacy endpoint: closes BOTH LEGS of the straddle strategy containing this position."""
    target_id = req.strategy_id or req.symbol
    if not target_id:
        raise HTTPException(status_code=400, detail="Missing contract symbol or strategy_id.")
    res = manager.close_strategy(target_id)
    return res


@app.post("/api/orders/{client_order_id}/cancel")
def cancel_order(client_order_id: str):
    """Cancel one cached order through the NautilusTrader strategy/ExecutionEngine."""
    result = manager.cancel_order(client_order_id)
    if result.get("status") == "ERROR":
        raise HTTPException(status_code=400, detail=result.get("message"))
    return result


@app.get("/api/orders")
def get_orders():
    """Return NautilusTrader's cached orders plus durable callback history."""
    return manager.order_history()


@app.get("/api/positions")
def get_positions():
    """Return only positions reconciled in NautilusTrader's live cache."""
    return manager.live_positions()


@app.post("/api/straddle/shock")
def simulate_shock(req: ShockRequest):
    """Demo helper: injects synthetic price move to trigger 1% SL or 2% TP."""
    try:
        target = req.strategy_id or req.symbol
        res = manager.simulate_price_shock(req.shock_type, strategy_id=target)
        if res.get("status") == "ERROR":
            raise HTTPException(status_code=400, detail=res.get("message"))
        return res
    except HTTPException:
        raise
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
                    "LiveExecEngine. NautilusTrader's native Bybit execution adapter transports the order using its "
                    "supported connection. The application does not construct or send exchange HTTP requests itself."
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
async def get_dashboard():
    """Serves the dashboard HTML interface."""
    template_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()


# -------------------------------------------------------------------------
# WebSocket Endpoint
# -------------------------------------------------------------------------

@app.websocket("/ws/stream")
async def websocket_endpoint(websocket: WebSocket):
    """Real-time event stream for the dashboard."""
    global main_event_loop
    main_event_loop = asyncio.get_running_loop()
    await websocket.accept()
    connected_websockets.append(websocket)
    try:
        # Send current snapshot immediately
        await websocket.send_text(json.dumps(manager.latest_telemetry, default=str))
        while True:
            # Keepalive listener
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if websocket in connected_websockets:
            connected_websockets.remove(websocket)
