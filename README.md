# BTC Options Short Straddle using NautilusTrader & Bybit

A systematic options trading strategy built on **NautilusTrader** and integrated with **Bybit Demo Trading & Testnet**.

## Strategy Overview

This implementation covers both stages of the Intern Engineering Challenge:
- **Level 1 – Naked Straddle**:
  - Connects to Bybit Demo Trading.
  - Automatically queries underlying BTC reference price ($S_{\text{ref}}$) and active option contracts.
  - Determines the nearest At-The-Money (ATM) strike.
  - Simultaneously sells 1 ATM Call and 1 ATM Put option with matching strike and expiry.
  - Tracks order submission, acknowledgments, execution fills, and position lifecycle.
- **Level 2 – Automated Risk Management**:
  - Computes combined entry premium ($V_{\text{entry}} = P_{\text{entry, call}} + P_{\text{entry, put}}$).
  - Continuously monitors combined buyback premium using **ask-side quotes**:
    $$V_{\text{buyback}} = P_{\text{ask, call}} + P_{\text{ask, put}}$$
  - **Stop-Loss (+1%)**: If buyback premium increases by 1% from entry, automatically closes both positions.
  - **Take-Profit (-2%)**: If buyback premium decreases by 2% from entry, automatically locks in profit and closes both positions.
  - Guards against duplicate exit attempts (idempotency).
  - Implements legging risk protocol (safely unwinds unhedged single-leg executions).

---

## Project Structure

```
├── straddle/
│   ├── config.py             # BTCShortStraddleConfig (Level 1 & Level 2 settings)
│   ├── strike_selector.py    # ATM strike discovery & Call/Put pairing engine
│   ├── strategy.py           # Core NautilusTrader BTCShortStraddleStrategy
│   └── execution_manager.py  # TradingNode & environment manager
├── web/
│   ├── app.py                # FastAPI backend with REST & WebSocket streaming
│   └── templates/
│       └── index.html        # Interactive Trading Terminal & Dashboard UI
├── tests/
│   ├── test_strike_selection.py  # ATM strike & contract pairing tests
│   ├── test_risk_management.py   # 1% SL & 2% TP calculation & idempotency tests
│   └── test_legging_risk.py      # Unhedged single-leg unwind tests
├── run_dashboard.py          # Web UI dashboard entrypoint
├── run_straddle.py           # Standalone CLI entrypoint
├── WALKTHROUGH.md            # Detailed answers to the 10 interview questions
├── .env.example              # Environment credentials template
└── requirements.txt          # Python dependencies
```

---

## Quick Start

### 1. Installation
Activate the virtual environment:
```powershell
# Windows PowerShell
.\.venv\Scripts\activate
```

### 2. Configure Credentials (Optional)
```powershell
copy .env.example .env
```
Add your Bybit API credentials to `.env`:
- **For Demo Trading (Recommended):** Use credentials generated in Demo Mode on `bybit.com` with `BYBIT_ENVIRONMENT=DEMO` (includes \$50,000 USDT practice balance).
- **For Standalone Testnet:** Use credentials from `testnet.bybit.com` with `BYBIT_ENVIRONMENT=TESTNET`.
- **For Mainnet Real Trading:** Use credentials from `bybit.com` with `BYBIT_ENVIRONMENT=MAINNET`.
- **Live-Market Simulation Mode:** If keys are omitted, the engine streams 100% real-time Bybit market quotes and emulates fills with realistic bid/ask spreads.

### 3. Launch the Web Trading Dashboard
```powershell
python run_dashboard.py --port 8000 --env DEMO
```
Open **`http://localhost:8000`** in your browser.

The dashboard features:
- Live BTC Reference Price ticker
- ATM Strike discovery & Options Chain table
- Strategy Control: Level 1 vs Level 2 toggle, order size selector
- Real-time quote streaming: Combined Entry Premium, Buyback Premium, PnL %, SL/TP bounds
- Execution visualizer & live event log stream
- **10-Question Interview Walkthrough Tab**

### 4. Run via Standalone CLI
```powershell
# Run Level 1 Naked Straddle
python run_straddle.py --level 1 --qty 0.01 --env DEMO

# Run Level 2 Automated 1% SL / 2% TP Straddle
python run_straddle.py --level 2 --qty 0.01 --env DEMO
```

### 5. Run the Test Suite
```powershell
python -m pytest -v tests
```

---

## 10-Question Walkthrough Guide

See the full written walkthrough and presentation guide in [WALKTHROUGH.md](WALKTHROUGH.md) or open the **"Interview Walkthrough"** tab in the web dashboard!

