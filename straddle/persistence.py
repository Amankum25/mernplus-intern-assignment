"""
Thread-safe disk persistence for trading session state.
Ensures active trades, orders, and logs survive process restarts.
Uses atomic file replacement to prevent corruption on unexpected termination.
"""

import json
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional

STATE_FILE = os.environ.get(
    "STRADDLE_STATE_FILE",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "session_state.json"),
)


class StatePersistence:
    """
    Thread-safe JSON persistence for trading session state.
    One global instance is shared across FastAPI and background strategy threads.
    """

    def __init__(self, filepath: str = STATE_FILE):
        self.filepath = filepath
        self.lock = threading.Lock()
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        self.state: Dict[str, Any] = self._load_initial_state()

    def _default_state(self) -> Dict[str, Any]:
        """Clean slate when starting with no prior data on disk."""
        return {
            "active_trade": None,
            "active_strategies": [],
            "orders": [],
            "trade_history": [],
            "logs": [],
        }

    def _load_initial_state(self) -> Dict[str, Any]:
        """Tries to load existing state from disk on startup."""
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        strategies = data.get("active_strategies", [])
                        active_trade = data.get("active_trade")
                        if not strategies and active_trade and isinstance(active_trade, dict):
                            if active_trade.get("status") == "OPEN" and active_trade.get("call_symbol") and active_trade.get("put_symbol"):
                                if "strategy_id" not in active_trade:
                                    active_trade["strategy_id"] = f"STRAT-{int(time.time()*1000)}"
                                if "name" not in active_trade:
                                    active_trade["name"] = "Strategy #1: Straddle"
                                if "created_ts" not in active_trade:
                                    active_trade["created_ts"] = time.time()
                                strategies = [active_trade]
                        return {
                            "active_trade": active_trade,
                            "active_strategies": strategies,
                            "orders": data.get("orders", []),
                            "trade_history": data.get("trade_history", []),
                            "logs": data.get("logs", []),
                        }
            except Exception:
                pass
        state = self._default_state()
        self._save_to_disk(state)
        return state

    def _save_to_disk(self, state: Dict[str, Any]) -> None:
        """Writes state to a temp file first, then atomically replaces the real file."""
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

    def get_active_trade(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            trade = self.state.get("active_trade")
            return dict(trade) if isinstance(trade, dict) else None

    def set_active_trade(self, trade_data: Dict[str, Any]) -> None:
        with self.lock:
            if "started_at" not in trade_data:
                trade_data["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            self.state["active_trade"] = dict(trade_data)
            self._save_to_disk(self.state)

    def update_active_trade(self, updates: Dict[str, Any]) -> None:
        with self.lock:
            if self.state.get("active_trade"):
                self.state["active_trade"].update(updates)
                self.state["active_trade"]["last_updated"] = time.strftime("%H:%M:%S")
                self._save_to_disk(self.state)

    def clear_active_trade(self) -> None:
        with self.lock:
            self.state["active_trade"] = None
            self.state["active_strategies"] = []
            self._save_to_disk(self.state)

    def _reindex_strategies_locked(self) -> None:
        """
        Reindexes active strategies in chronological order:
        Oldest = Strategy #1, next = Strategy #2, etc.
        """
        if "active_strategies" not in self.state or not self.state["active_strategies"]:
            return
        sorted_strats = sorted(
            self.state["active_strategies"],
            key=lambda s: float(s.get("created_ts", 0.0) or 0.0)
        )
        for idx, s in enumerate(sorted_strats, start=1):
            strike = s.get("strike")
            strike_str = f"${float(strike):,.0f} " if strike else ""
            s["name"] = f"Strategy #{idx}: {strike_str}Straddle"

    def get_active_strategies(self) -> List[Dict[str, Any]]:
        with self.lock:
            self._reindex_strategies_locked()
            strats = list(self.state.get("active_strategies", []))
            # Return in descending order of time (newest on top)
            return sorted(strats, key=lambda s: float(s.get("created_ts", 0.0) or 0.0), reverse=True)

    def add_active_strategy(self, strat_data: Dict[str, Any]) -> Dict[str, Any]:
        with self.lock:
            if "active_strategies" not in self.state:
                self.state["active_strategies"] = []
            now_ts = strat_data.get("created_ts") or time.time()
            strat = dict(strat_data)
            strat["created_ts"] = now_ts
            if "started_at" not in strat:
                strat["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            if "time_str" not in strat:
                strat["time_str"] = time.strftime("%H:%M:%S")
            if "strategy_id" not in strat:
                strat["strategy_id"] = f"STRAT-{int(now_ts * 1000)}"
            if "name" not in strat:
                num = len(self.state["active_strategies"]) + 1
                strat["name"] = f"Strategy #{num}: Straddle"

            # Remove duplicate if exists
            self.state["active_strategies"] = [s for s in self.state["active_strategies"] if s.get("strategy_id") != strat["strategy_id"]]
            self.state["active_strategies"].insert(0, strat)
            self._reindex_strategies_locked()
            self.state["active_trade"] = dict(strat)
            self._save_to_disk(self.state)
            return strat

    def update_active_strategy(self, strategy_id: str, updates: Dict[str, Any]) -> None:
        with self.lock:
            for s in self.state.get("active_strategies", []):
                if s.get("strategy_id") == strategy_id:
                    s.update(updates)
                    s["last_updated"] = time.strftime("%H:%M:%S")
            if self.state.get("active_trade") and self.state["active_trade"].get("strategy_id") == strategy_id:
                self.state["active_trade"].update(updates)
                self.state["active_trade"]["last_updated"] = time.strftime("%H:%M:%S")
            self._save_to_disk(self.state)

    def remove_active_strategy(self, strategy_id_or_symbol: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            removed = None
            remaining = []
            for s in self.state.get("active_strategies", []):
                if (s.get("strategy_id") == strategy_id_or_symbol or 
                    s.get("call_symbol") == strategy_id_or_symbol or 
                    s.get("put_symbol") == strategy_id_or_symbol):
                    removed = s
                else:
                    remaining.append(s)
            self.state["active_strategies"] = remaining
            self._reindex_strategies_locked()
            if remaining:
                self.state["active_trade"] = dict(remaining[0])
            else:
                self.state["active_trade"] = None
            self._save_to_disk(self.state)
            return removed

    def clear_all_active_strategies(self) -> None:
        with self.lock:
            self.state["active_strategies"] = []
            self.state["active_trade"] = None
            self._save_to_disk(self.state)

    def add_order(
        self,
        order_id: str,
        contract: str,
        side: str,
        qty: float,
        filled_qty: float,
        avg_price: float,
        status: str,
        time_str: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self.lock:
            if "orders" not in self.state:
                self.state["orders"] = []

            now_str = time_str or time.strftime("%H:%M:%S")
            existing = next((o for o in self.state["orders"] if o.get("order_id") == order_id), None)
            if existing:
                existing.update({
                    "filled_qty": filled_qty,
                    "avg_price": avg_price,
                    "status": status.upper(),
                    "updated_at": now_str,
                })
                order_entry = existing
            else:
                order_entry = {
                    "order_id": order_id,
                    "time": now_str,
                    "contract": contract,
                    "side": side.upper(),
                    "qty": qty,
                    "filled_qty": filled_qty,
                    "avg_price": avg_price,
                    "status": status.upper(),
                }
                self.state["orders"].append(order_entry)
                if len(self.state["orders"]) > 200:
                    self.state["orders"] = self.state["orders"][-200:]
            self._save_to_disk(self.state)
            return order_entry

    def get_orders(self) -> List[Dict[str, Any]]:
        with self.lock:
            return list(self.state.get("orders", []))

    def add_log(
        self,
        category: str,
        message: str,
        badge_type: str = "info",
        time_str: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self.lock:
            if "logs" not in self.state:
                self.state["logs"] = []
            log_entry = {
                "id": len(self.state["logs"]) + 1,
                "time": time_str or time.strftime("%H:%M:%S"),
                "category": category.upper(),
                "message": message,
                "badge_type": badge_type,
            }
            self.state["logs"].append(log_entry)
            if len(self.state["logs"]) > 300:
                self.state["logs"] = self.state["logs"][-300:]
            self._save_to_disk(self.state)
            return log_entry

    def get_logs(self) -> List[Dict[str, Any]]:
        with self.lock:
            return list(self.state.get("logs", []))

    def clear_logs(self) -> None:
        with self.lock:
            self.state["logs"] = []
            self._save_to_disk(self.state)

    def get_full_state(self) -> Dict[str, Any]:
        with self.lock:
            return json.loads(json.dumps(self.state))


# Global shared instance
state_persistence = StatePersistence()

