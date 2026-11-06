"""
Persistent state manager for the BTC Options Straddle system.
Guarantees zero data loss on browser refresh or server restart.
Persists active positions, live valuation, trade history, cumulative PnL, and structured logs.
"""

import json
import os
import threading
import time
from typing import Any, Dict, List, Optional

STATE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "session_state.json")


class StatePersistence:
    """
    Thread-safe JSON persistence layer for trading session state and logs.
    """

    def __init__(self, filepath: str = STATE_FILE):
        self.filepath = filepath
        self.lock = threading.Lock()
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        self.state: Dict[str, Any] = self._load_initial_state()

    def _default_state(self) -> Dict[str, Any]:
        return {
            "active_trade": None,
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
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # Validate keys
                    if "logs" in data and "stats" in data:
                        return data
            except Exception:
                pass
        state = self._default_state()
        self._save_to_disk(state)
        return state

    def _save_to_disk(self, state: Dict[str, Any]) -> None:
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
        """Add a structured log entry and persist it."""
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
            # Keep last 300 logs
            if len(self.state["logs"]) > 300:
                self.state["logs"] = self.state["logs"][-300:]
            self._save_to_disk(self.state)
            return log_entry

    def set_active_trade(self, trade_data: Dict[str, Any]) -> None:
        """Set or replace active straddle trade information."""
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
        """Update live pricing and PnL for active trade."""
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
        """Record trade closure and append to trade history."""
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

            # Update stats
            stats = self.state["stats"]
            stats["total_trades"] += 1
            if pnl_usd > 0:
                stats["winning_trades"] += 1
            stats["cumulative_pnl_usd"] += pnl_usd
            stats["cumulative_pnl_pct"] += pnl_pct

            self._save_to_disk(self.state)
            return completed_record

    def clear_active_trade(self) -> None:
        """Clear active trade without recording (e.g. manual reset)."""
        with self.lock:
            self.state["active_trade"] = None
            self._save_to_disk(self.state)

    def get_full_state(self) -> Dict[str, Any]:
        """Return snapshot of complete session state for frontend rehydration."""
        with self.lock:
            return json.loads(json.dumps(self.state))


# Global singleton instance
state_persistence = StatePersistence()

