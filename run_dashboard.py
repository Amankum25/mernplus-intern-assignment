"""
Entry point for the web dashboard.
Starts a Uvicorn ASGI server and serves the FastAPI app on the configured port.

Usage:
    python run_dashboard.py --port 8000 --env DEMO
"""

import argparse
import os
import uvicorn
from dotenv import load_dotenv

load_dotenv()


def main():
    # parse CLI args for port, host, and environment
    parser = argparse.ArgumentParser(description="BTC Options Straddle Web Dashboard")
    parser.add_argument("--port", type=int, default=int(os.getenv("WEB_PORT", 8000)), help="Port to run server on")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host interface")
    parser.add_argument("--env", type=str, default=os.getenv("BYBIT_ENVIRONMENT", "DEMO"), help="DEMO or TESTNET")

    args = parser.parse_args()

    # pass env to web/app.py via environment variable so it's available on import
    os.environ["BYBIT_ENVIRONMENT"] = args.env

    print("=" * 70)
    print("  NAUTILUSTRADER - BTC OPTIONS SHORT STRADDLE DASHBOARD")
    print(f"  Target Environment : Bybit {args.env}")
    print(f"  Dashboard URL      : http://{args.host}:{args.port}")
    print("=" * 70)

    # start the ASGI server — reload=False for production stability
    uvicorn.run("web.app:app", host=args.host, port=args.port, reload=False, log_level="info")


if __name__ == "__main__":
    main()
