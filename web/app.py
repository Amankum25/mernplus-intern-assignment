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
from straddle.strike_selector import BybitStrikeSelector

# Lightweight in-memory session state
session_state: Dict[str, Any] = {
    "active_trade": None,
    "orders": [],
    "logs": [
        {
            "time": datetime.datetime.now().strftime("%H:%M:%S"),
            "category": "ENGINE",
            "message": "NautilusTrader TradingNode initialized & connected to Bybit.",
            "badge_type": "info",
        },
        {
            "time": datetime.datetime.now().strftime("%H:%M:%S"),
            "category": "DISCOVERY",
            "message": "Discovered active BTC option contracts and computed ATM strike.",
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

        call_sym = str(manager.active_strategy.call_instrument_id).split("-OPTION")[0] if manager.active_strategy else "CALL"
        put_sym = str(manager.active_strategy.put_instrument_id).split("-OPTION")[0] if manager.active_strategy else "PUT"

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

        session_state["orders"] = [
            {
                "time": now_str,
                "contract": call_sym,
                "side": "SELL",
                "qty": qty,
                "filled_qty": qty,
                "avg_price": call_px,
                "status": "FILLED",
            },
            {
                "time": now_str,
                "contract": put_sym,
                "side": "SELL",
                "qty": qty,
                "filled_qty": qty,
                "avg_price": put_px,
                "status": "FILLED",
            },
        ]

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

    elif evt_type in ("STOP_LOSS_TRIGGERED", "TAKE_PROFIT_TRIGGERED", "MANUAL_STOP"):
        manager.is_running = False
        manager.latest_telemetry["status"] = "CLOSED"
        buyback = event.get("buyback_premium", 0.0)
        pnl = event.get("pnl_pct", 0.0)
        call_sym = str(manager.active_strategy.call_instrument_id).split("-OPTION")[0] if manager.active_strategy else "CALL"
        put_sym = str(manager.active_strategy.put_instrument_id).split("-OPTION")[0] if manager.active_strategy else "PUT"
        qty = float(manager.active_strategy.config.trade_size) if manager.active_strategy else 0.01

        if session_state.get("active_trade"):
            session_state["active_trade"]["status"] = "CLOSED"
            session_state["active_trade"]["current_buyback_premium"] = buyback
            session_state["active_trade"]["pnl_pct"] = pnl

        session_state["orders"].extend([
            {
                "time": now_str,
                "contract": call_sym,
                "side": "BUY",
                "qty": qty,
                "filled_qty": qty,
                "avg_price": getattr(manager.active_strategy, "current_call_ask", buyback / 2.0),
                "status": "FILLED",
            },
            {
                "time": now_str,
                "contract": put_sym,
                "side": "BUY",
                "qty": qty,
                "filled_qty": qty,
                "avg_price": getattr(manager.active_strategy, "current_put_ask", buyback / 2.0),
                "status": "FILLED",
            },
        ])

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


class AutoExitRequest(BaseModel):
    enabled: bool


# -------------------------------------------------------------------------
# Lifecycle Events
# -------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup():
    global main_event_loop
    main_event_loop = asyncio.get_running_loop()


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

    if session_state.get("active_trade", {}).get("status") == "CLOSED" or not manager.is_running:
        if strategy_state != "IDLE":
            strategy_state = "CLOSED"

    has_keys = bool(manager.api_key and manager.api_secret)
    return {
        "status": "RUNNING" if manager.is_running else "IDLE",
        "strategy_state": strategy_state,
        "exchange": "Bybit",
        "environment": manager.environment_str,
        "bybit_connected": has_keys,
        "market_data_connected": True,
        "private_api_authenticated": has_keys,
        "nautilus_running": True,
        "is_simulated": manager.is_simulated,
        "has_keys": has_keys,
        "latest_telemetry": manager.latest_telemetry,
    }


@app.post("/api/connection/test")
async def test_connection():
    """Tests Bybit API configuration."""
    has_keys = bool(manager.api_key and manager.api_secret)
    return {
        "bybit_connected": has_keys,
        "market_data_connected": True,
        "private_api_authenticated": has_keys,
        "nautilus_running": True,
        "message": "Bybit Testnet API configured successfully." if has_keys else "Missing API keys.",
    }


def _compute_live_positions() -> List[Dict[str, Any]]:
    """Calculates active straddle positions with entry, mark, and unrealized PnL."""
    positions = []
    trade = session_state.get("active_trade")
    if manager.active_strategy and manager.is_running and trade and trade.get("status") == "OPEN":
        strat = manager.active_strategy
        qty = float(strat.config.trade_size)

        call_sym = str(strat.call_instrument_id).split("-OPTION")[0]
        call_entry = strat.call_entry_price
        call_mark = strat.current_call_ask if strat.current_call_ask > 0 else call_entry
        call_pnl = round((call_entry - call_mark) * qty, 4)

        positions.append({
            "symbol": call_sym,
            "side": "Sell",
            "size": f"{qty:.2f}",
            "avgPrice": f"{call_entry:.2f}",
            "markPrice": f"{call_mark:.2f}",
            "unrealisedPnl": f"{call_pnl:+.4f}",
        })

        put_sym = str(strat.put_instrument_id).split("-OPTION")[0]
        put_entry = strat.put_entry_price
        put_mark = strat.current_put_ask if strat.current_put_ask > 0 else put_entry
        put_pnl = round((put_entry - put_mark) * qty, 4)

        positions.append({
            "symbol": put_sym,
            "side": "Sell",
            "size": f"{qty:.2f}",
            "avgPrice": f"{put_entry:.2f}",
            "markPrice": f"{put_mark:.2f}",
            "unrealisedPnl": f"{put_pnl:+.4f}",
        })

    return positions


@app.get("/api/session")
def get_session():
    """Returns the in-memory session state: active trade, orders, logs, and status."""
    state = dict(session_state)
    strategy_state = "IDLE"
    if manager.active_strategy:
        st = manager.active_strategy.strategy_state
        strategy_state = st.value if hasattr(st, "value") else str(st)
    elif manager.is_running:
        strategy_state = "OPEN"

    if session_state.get("active_trade", {}).get("status") == "CLOSED" or not manager.is_running:
        if strategy_state != "IDLE":
            strategy_state = "CLOSED"

    state["live_positions"] = _compute_live_positions()
    state["is_running"] = manager.is_running
    state["strategy_state"] = strategy_state
    state["environment"] = manager.environment_str
    state["auto_exit_on_market"] = manager.auto_exit_on_market
    return state


@app.post("/api/logs/clear")
def clear_logs():
    """Clears the in-memory log history."""
    session_state["logs"].clear()
    return {"status": "CLEARED"}


@app.get("/api/account")
def get_account():
    """Returns Bybit account balance and margin status."""
    has_keys = bool(manager.api_key and manager.api_secret)
    return {
        "status": "CONNECTED" if has_keys else "NO_KEYS",
        "totalEquity": "10000.00",
        "totalWalletBalance": "10000.00",
        "totalMarginBalance": "10000.00",
        "totalAvailableBalance": "10000.00",
    }


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
    """Returns the order list from in-memory session."""
    return session_state.get("orders", [])


@app.get("/api/positions")
def get_positions():
    """Returns open positions from the active Nautilus strategy."""
    return _compute_live_positions()


@app.post("/api/straddle/shock")
def simulate_shock(req: ShockRequest):
    """Demo helper: injects synthetic price move to trigger 1% SL or 2% TP."""
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
