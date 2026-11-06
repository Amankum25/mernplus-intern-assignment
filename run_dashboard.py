"""
Run the NautilusTrader BTC Options Straddle FastAPI Dashboard.
Usage:
    python run_dashboard.py --port 8000 --env DEMO
"""

import argparse
import os
import uvicorn
from dotenv import load_dotenv

# Load .env if present
load_dotenv()


def main():
    parser = argparse.ArgumentParser(description="BTC Options Straddle Web Dashboard")
    parser.add_argument("--port", type=int, default=int(os.getenv("WEB_PORT", 8000)), help="Port to run server on")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host interface")
    parser.add_argument("--env", type=str, default=os.getenv("BYBIT_ENVIRONMENT", "DEMO"), help="DEMO or TESTNET")

    args = parser.parse_args()

    os.environ["BYBIT_ENVIRONMENT"] = args.env

    print("=" * 70)
    print("  NAUTILUSTRADER - BTC OPTIONS SHORT STRADDLE DASHBOARD")
    print(f"  Target Environment : Bybit {args.env}")
    print(f"  Dashboard URL      : http://{args.host}:{args.port}")
    print("=" * 70)

    uvicorn.run("web.app:app", host=args.host, port=args.port, reload=False, log_level="info")


if __name__ == "__main__":
    main()

