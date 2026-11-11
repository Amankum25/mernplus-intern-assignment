"""
Command-line entry point for running the strategy without the web dashboard.
Useful for headless testing or running on a server.

Usage:
    python run_straddle.py --level 2 --qty 0.01 --env DEMO
"""

import argparse
import os
import sys
import time
from dotenv import load_dotenv

from straddle.execution_manager import StraddleExecutionManager

load_dotenv()


def main():
    # parse CLI args
    parser = argparse.ArgumentParser(description="NautilusTrader BTC Options Straddle CLI Runner")
    parser.add_argument("--level", type=int, default=2, choices=[1, 2], help="1: Naked Straddle, 2: Automated SL/TP")
    parser.add_argument("--qty", type=float, default=float(os.getenv("DEFAULT_TRADE_QTY", 0.01)), help="Trade size in BTC")
    parser.add_argument("--env", type=str, default=os.getenv("BYBIT_ENVIRONMENT", "DEMO"), choices=["DEMO", "TESTNET", "MAINNET"], help="Bybit environment")
    parser.add_argument("--simulated", action="store_true", help="Run with simulated fills against live market quotes")

    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("   NAUTILUSTRADER BTC SHORT STRADDLE STRATEGY RUNNER")
    print(f"   Mode         : Level {args.level} ({'Naked Straddle' if args.level == 1 else 'Automated 1% SL / 2% TP'})")
    print(f"   Environment  : Bybit {args.env}")
    print(f"   Order Size   : {args.qty} BTC per leg")
    print("=" * 70 + "\n")

    # 1. query Bybit for current BTC price and nearest ATM straddle pair
    manager = StraddleExecutionManager(environment=args.env)

    print("[1/4] Discovering BTC Reference Price & Options Chain from Bybit...")
    market = manager.get_market_overview()
    print(f"      -> Current BTC Reference Price: ${market['btc_price']:,.2f}")
    print(f"      -> Target Expiration: {market['expiry_str']}")
    print(f"      -> Nearest ATM Strike: ${market['atm_strike']:,.2f}")
    print(f"      -> Selected Call Leg: {market['call_contract']['symbol']}")
    print(f"      -> Selected Put Leg:  {market['put_contract']['symbol']}\n")

    # 2. register a console listener so strategy events print to stdout
    print(f"[2/4] Initializing Straddle Strategy [Level {args.level}]...")

    def console_listener(event):
        """Prints strategy lifecycle events directly to the terminal."""
        t_type = event.get("type")
        if t_type == "ORDER_SUBMITTED":
            print(f"  [ORDER SUBMITTED] {event.get('order_side')} {event.get('client_order_id')} on {event.get('instrument_id')}")
        elif t_type == "ORDER_ACCEPTED":
            print(f"  [ORDER ACKNOWLEDGED] {event.get('client_order_id')} -> Venue ID: {event.get('venue_order_id')}")
        elif t_type == "ORDER_FILLED":
            print(f"  [ORDER EXECUTED] {event.get('client_order_id')} @ ${event.get('fill_price'):.2f} (Qty: {event.get('fill_qty')})")
        elif t_type == "STRADDLE_POSITION_ESTABLISHED":
            print("\n" + "*" * 60)
            print(f"  [STRADDLE ACTIVE] Combined Entry Premium: ${event.get('combined_entry_premium'):.2f}")
            if args.level == 2:
                print(f"  [RISK BOUNDS] Stop-Loss (+1%): ${event.get('stop_loss_target'):.2f} | Take-Profit (-2%): ${event.get('take_profit_target'):.2f}")
            print("*" * 60 + "\n")
        elif t_type == "VALUATION_UPDATE":
            # live ticking line — overwrites itself in-place using \r
            pnl = event.get("pnl_pct", 0.0)
            pnl_sign = "+" if pnl >= 0 else ""
            sys.stdout.write(
                f"\r  [TICK] Buyback: ${event.get('current_buyback_premium'):.2f} "
                f"(Ask C: ${event.get('call_ask'):.1f} | Ask P: ${event.get('put_ask'):.1f}) "
                f"| PnL: {pnl_sign}{pnl:.2f}% (${event.get('pnl_usd', 0):.2f})  "
            )
            sys.stdout.flush()
        elif t_type == "EXIT_TRIGGERED":
            print(f"\n\n  >>> [EXIT TRIGGERED] {event.get('reason')} <<<")
            print(f"      Buyback: ${event.get('current_buyback_premium'):.2f} vs Entry: ${event.get('combined_entry_premium'):.2f}")
            print(f"      Net PnL: {event.get('pnl_pct'):.2f}%")
        elif t_type == "STRADDLE_CLOSED":
            print(f"  [STRADDLE COMPLETED] All positions successfully closed! Reason: {event.get('reason')}\n")

    manager.register_listener(console_listener)

    # 3. start the strategy — this spawns the background market loop thread
    print("[3/4] Launching Execution Engine & Live Market Feed...")
    manager.start_straddle(
        level=args.level,
        trade_size=args.qty,
        simulated=args.simulated or (not os.getenv("BYBIT_API_KEY")),
    )

    print("[4/4] Strategy is running. Press Ctrl+C to stop.\n")

    # 4. keep the main thread alive; Ctrl+C triggers graceful shutdown
    try:
        while manager.is_running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nStopping strategy and closing open positions...")
        manager.stop_straddle()
        print("Shutdown complete.")


if __name__ == "__main__":
    main()
