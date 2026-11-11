"""
Entry point for the web dashboard.
Starts a Uvicorn ASGI server and serves the FastAPI app on the configured port.

Usage:
    python run_dashboard.py --port 8000 --env DEMO
"""

import argparse
import os
import subprocess
import time
import uvicorn
from dotenv import load_dotenv

load_dotenv()


def free_tcp_port(port: int) -> None:
    """Free a TCP port if it's already bound by an old orphaned process."""
    if os.name == "nt":
        try:
            result = subprocess.run(
                ["netstat", "-ano", "-p", "tcp"],
                capture_output=True,
                text=True,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            if result.returncode == 0:
                pids = set()
                for line in result.stdout.splitlines():
                    fields = line.split()
                    if len(fields) >= 5 and fields[0].upper() == "TCP":
                        local_endpoint = fields[1]
                        state = fields[-2].upper()
                        pid_text = fields[-1]
                        if state == "LISTENING" and local_endpoint.rsplit(":", 1)[-1] == str(port):
                            try:
                                pid = int(pid_text)
                                if pid != os.getpid():
                                    pids.add(pid)
                            except ValueError:
                                pass
                for pid in pids:
                    print(f"Stopping lingering process {pid} on port {port}...")
                    subprocess.run(
                        ["taskkill", "/PID", str(pid), "/T", "/F"],
                        capture_output=True,
                        text=True,
                        check=False,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                if pids:
                    time.sleep(1.0)
        except Exception as e:
            print(f"Notice during port cleanup: {e}")


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
    free_tcp_port(args.port)
    uvicorn.run("web.app:app", host=args.host, port=args.port, reload=False, log_level="info")


if __name__ == "__main__":
    main()
