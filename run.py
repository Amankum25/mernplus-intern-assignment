"""
Master all-in-one runner for the BTC Options Straddle project.

Usage:
    python run.py                 # (Default) Runs tests, checks market, and starts Dashboard
    python run.py --web           # Starts only the Web Dashboard
    python run.py --test          # Runs only the unit test suite
    python run.py --cli           # Runs the command-line strategy directly (Level 2)
    python run.py --cli --level 1 # Runs the command-line strategy in Level 1 (Naked)
"""

import argparse
import os
import subprocess
import sys
import time
import webbrowser
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# 1. Automatic Virtual Environment Forwarding
# -----------------------------------------------------------------------------
# If invoked using system Python instead of .venv Python, automatically
# re-executes using the project's virtual environment python.exe.
VENV_PYTHON = os.path.join(os.path.dirname(__file__), ".venv", "Scripts", "python.exe")
if os.path.exists(VENV_PYTHON) and sys.executable.lower() != os.path.abspath(VENV_PYTHON).lower():
    result = subprocess.run([VENV_PYTHON, __file__] + sys.argv[1:])
    sys.exit(result.returncode)


def run_tests() -> bool:
    """Runs the 5-point unit test suite and reports status."""
    print("\n" + "=" * 70)
    print("  [STEP 1/3] RUNNING UNIT TEST SUITE")
    print("=" * 70)
    cmd = [sys.executable, "-m", "pytest", "tests/test_straddle.py", "-v", "--no-header"]
    res = subprocess.run(cmd)
    if res.returncode == 0:
        print("  -> ALL 5 UNIT TESTS PASSED SUCCESSFULLY! [OK]\n")
        return True
    else:
        print("  -> TEST FAILURE DETECTED. Review errors above.\n")
        return False


def show_market_check(env: str = "DEMO") -> None:
    """Queries Bybit reference price and current ATM strike."""
    print("=" * 70)
    print("  [STEP 2/3] BYBIT LIVE MARKET DISCOVERY")
    print("=" * 70)
    try:
        from straddle.execution_manager import StraddleExecutionManager
        mgr = StraddleExecutionManager(environment=env)
        overview = mgr.get_market_overview()
        print(f"  Underlying BTC Price : ${overview['btc_price']:,.2f}")
        print(f"  Nearest ATM Strike   : ${overview['atm_strike']:,.2f}")
        print(f"  Target Expiry        : {overview['expiry_str']}")
        print(f"  Selected Call Leg    : {overview['call_contract']['symbol']}")
        print(f"  Selected Put Leg     : {overview['put_contract']['symbol']}")
        print("  -> Live Market Connection: ACTIVE [OK]\n")
    except Exception as e:
        print(f"  -> Market query notice: {e}\n")


def launch_dashboard(port: int = 8000, host: str = "127.0.0.1", env: str = "DEMO") -> None:
    """Starts the FastAPI Uvicorn server and opens browser."""
    print("=" * 70)
    print("  [STEP 3/3] LAUNCHING WEB DASHBOARD")
    print("=" * 70)
    print(f"  Dashboard URL : http://{host}:{port}")
    print(f"  Environment   : Bybit {env}")
    print("  Press Ctrl+C to stop the server anytime.")
    print("=" * 70 + "\n")

    os.environ["BYBIT_ENVIRONMENT"] = env

    # Auto-open browser after a short delay
    def open_browser():
        time.sleep(1.2)
        try:
            webbrowser.open(f"http://{host}:{port}")
        except Exception:
            pass

    import threading
    threading.Thread(target=open_browser, daemon=True).start()

    import uvicorn
    uvicorn.run("web.app:app", host=host, port=port, reload=False, log_level="info")


def run_cli(level: int = 2, qty: float = 0.01, env: str = "DEMO") -> None:
    """Runs the CLI headless runner directly."""
    cmd = [
        sys.executable,
        "run_straddle.py",
        "--level", str(level),
        "--qty", str(qty),
        "--env", env,
    ]
    subprocess.run(cmd)


def main():
    parser = argparse.ArgumentParser(description="BTC Options Straddle All-in-One Runner")
    parser.add_argument("--test", action="store_true", help="Run only the test suite")
    parser.add_argument("--web", action="store_true", help="Launch only the web dashboard")
    parser.add_argument("--cli", action="store_true", help="Run the strategy in terminal CLI mode")
    parser.add_argument("--level", type=int, default=2, choices=[1, 2], help="1: Naked Straddle, 2: Automated SL/TP")
    parser.add_argument("--qty", type=float, default=0.01, help="Order quantity per leg in BTC")
    parser.add_argument("--port", type=int, default=8000, help="Web dashboard port")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Web dashboard host")
    parser.add_argument("--env", type=str, default="DEMO", choices=["DEMO", "TESTNET", "MAINNET"], help="Bybit environment")

    args = parser.parse_args()

    # Mode 1: Run tests only
    if args.test:
        success = run_tests()
        sys.exit(0 if success else 1)

    # Mode 2: Run CLI only
    if args.cli:
        run_cli(level=args.level, qty=args.qty, env=args.env)
        return

    # Mode 3: Run Web dashboard only
    if args.web:
        launch_dashboard(port=args.port, host=args.host, env=args.env)
        return

    # Default Mode: The complete all-in-one experience!
    # 1. Run unit tests to verify system health
    # 2. Query Bybit market price & ATM contracts
    # 3. Boot web dashboard and open browser
    print("\n" + "#" * 70)
    print("   BTC OPTIONS SHORT STRADDLE — ALL-IN-ONE SYSTEM LAUNCHER")
    print("#" * 70)

    test_passed = run_tests()
    if not test_passed:
        print("Tests failed. Do you still want to start the dashboard? (Ctrl+C to abort)")
        time.sleep(2)

    show_market_check(env=args.env)
    launch_dashboard(port=args.port, host=args.host, env=args.env)


if __name__ == "__main__":
    main()

