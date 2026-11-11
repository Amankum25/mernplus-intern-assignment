"""
Saves trade state to disk so nothing is lost if the server restarts.
All writes go through a lock (thread-safe) and use atomic file replacement
to prevent corruption if the process crashes mid-write.
"""

import json
import os
import threading
import time
from typing import Any, Dict, List, Optional

STATE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "session_state.json")


class StatePersistence:
    """
    Thread-safe JSON persistence for trading session state.
    One global instance is shared across the FastAPI server and background strategy thread.
    """

    def __init__(self, filepath: str = STATE_FILE):
        self.filepath = filepath
        self.lock = threading.Lock()
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        self.state: Dict[str, Any] = self._load_initial_state()

    def _default_state(self) -> Dict[str, Any]:
        """Clean slate when starting a fresh session with no prior data on disk."""
        return {
            "active_trade": None,
            "orders": [],
            "trade_history": [],
            "logs": [],
            "stats": {
                "total_trades": 0,
                "winning_trades": 0,
                "cumulative_pnl_usd": 0.0,
                "cumulative_pnl_pct": 0.0,
            },
        }

    def _load_initial_state(self) -> Dict[str, Any]:
        """
        Tries to load existing state from disk on startup.
        If the file is missing or corrupted, we start with a clean default state.
        """
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if "logs" in data and "stats" in data:
                        return data
            except Exception:
                pass
        state = self._default_state()
        self._save_to_disk(state)
        return state

    def _save_to_disk(self, state: Dict[str, Any]) -> None:
        """
        Writes state to a temp file first, then atomically replaces the real file.
        This way a crash mid-write never leaves a partially-written (corrupt) state file.
        """
        try:
            tmp_path = f"{self.filepath}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, default=str)
            if os.path.exists(self.filepath):
                os.replace(tmp_path, self.filepath)
            else:
                os.rename(tmp_path, self.filepath)
        except Exception as e:
            print(f"[StatePersistence] Failed to write state: {e}")

    def add_log(
        self,
        category: str,
        message: str,
        badge_type: str = "info",
        pnl_pct: Optional[float] = None,
        pnl_usd: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Appends a structured log entry and immediately persists it to disk.
        We cap the buffer at 300 entries so memory doesn't grow unbounded over long sessions.
        """
        with self.lock:
            log_entry = {
                "id": len(self.state["logs"]) + 1,
                "time": time.strftime("%H:%M:%S"),
                "timestamp_ms": int(time.time() * 1000),
                "category": category.upper(),
                "message": message,
                "badge_type": badge_type,
                "pnl_pct": pnl_pct,
                "pnl_usd": pnl_usd,
            }
            self.state["logs"].append(log_entry)
            if len(self.state["logs"]) > 300:
                self.state["logs"] = self.state["logs"][-300:]
            self._save_to_disk(self.state)
            return log_entry

    def set_active_trade(self, trade_data: Dict[str, Any]) -> None:
        """Stores the current open straddle trade. Overwrites any previous active trade."""
        with self.lock:
            trade_data["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            trade_data["is_active"] = True
            self.state["active_trade"] = trade_data
            self._save_to_disk(self.state)

    def update_valuation(
        self,
        call_ask: float,
        call_bid: float,
        put_ask: float,
        put_bid: float,
        current_buyback_premium: float,
        pnl_pct: float,
        pnl_usd: float,
    ) -> None:
        """Updates the live bid/ask prices and PnL on the active trade. Called every 5 seconds."""
        with self.lock:
            if self.state["active_trade"]:
                self.state["active_trade"].update(
                    {
                        "call_ask": call_ask,
                        "call_bid": call_bid,
                        "put_ask": put_ask,
                        "put_bid": put_bid,
                        "current_buyback_premium": current_buyback_premium,
                        "pnl_pct": pnl_pct,
                        "pnl_usd": pnl_usd,
                        "last_updated": time.strftime("%H:%M:%S"),
                    }
                )
                self._save_to_disk(self.state)

    def close_active_trade(self, reason: str, exit_buyback: float, pnl_pct: float, pnl_usd: float) -> Optional[Dict[str, Any]]:
        """
        Archives the completed trade into history and updates session-level win/loss stats.
        Called when the strategy exits via SL, TP, or manual stop.
        """
        with self.lock:
            trade = self.state["active_trade"]
            if not trade:
                return None

            completed_record = {
                "id": f"TRD-{int(time.time() * 1000)}",
                "level": trade.get("level", 2),
                "atm_strike": trade.get("atm_strike", 0.0),
                "expiry_str": trade.get("expiry_str", ""),
                "combined_entry_premium": trade.get("combined_entry_premium", 0.0),
                "exit_buyback_premium": exit_buyback,
                "pnl_pct": pnl_pct,
                "pnl_usd": pnl_usd,
                "exit_reason": reason,
                "opened_at": trade.get("started_at", ""),
                "closed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

            self.state["trade_history"].append(completed_record)
            self.state["active_trade"] = None

            stats = self.state["stats"]
            stats["total_trades"] += 1
            if pnl_usd > 0:
                stats["winning_trades"] += 1
            stats["cumulative_pnl_usd"] += pnl_usd
            stats["cumulative_pnl_pct"] += pnl_pct

            self._save_to_disk(self.state)
            return completed_record

    def add_order(
        self,
        order_id: str,
        contract: str,
        side: str,
        qty: float,
        filled_qty: float,
        avg_price: float,
        status: str,
    ) -> Dict[str, Any]:
        """
        Upserts an order into the persistent audit log.
        If an order with the same ID already exists (e.g. SUBMITTED → FILLED), we update it in-place.
        Otherwise we create a new record and cap the list at 100 entries.
        """
        with self.lock:
            if "orders" not in self.state:
                self.state["orders"] = []

            existing = next((o for o in self.state["orders"] if o["order_id"] == order_id), None)
            if existing:
                existing.update({
                    "filled_qty": filled_qty,
                    "avg_price": avg_price,
                    "status": status,
                    "updated_at": time.strftime("%H:%M:%S"),
                })
                order_entry = existing
            else:
                order_entry = {
                    "order_id": order_id,
                    "time": time.strftime("%H:%M:%S"),
                    "contract": contract,
                    "side": side.upper(),
                    "qty": qty,
                    "filled_qty": filled_qty,
                    "avg_price": avg_price,
                    "status": status.upper(),
                }
                self.state["orders"].append(order_entry)
                if len(self.state["orders"]) > 100:
                    self.state["orders"] = self.state["orders"][-100:]
            self._save_to_disk(self.state)
            return order_entry

    def clear_active_trade(self) -> None:
        """Clears the active trade without recording it to history. Used for manual resets."""
        with self.lock:
            self.state["active_trade"] = None
            self._save_to_disk(self.state)

    def get_full_state(self) -> Dict[str, Any]:
        """
        Returns a deep copy of the full state dict.
        We serialize + deserialize via JSON to avoid exposing internal references to callers.
        """
        with self.lock:
            return json.loads(json.dumps(self.state))


# Single shared instance used by both the FastAPI server and the execution manager
state_persistence = StatePersistence()
