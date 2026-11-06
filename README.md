# mernplus intern assignment
## BTC Options Short Straddle using NautilusTrader & Bybit Testnet

A production-grade quantitative trading system built with **NautilusTrader** and integrated with **Bybit Testnet / Demo Trading**. Features a clean, institutional fintech Light-Theme dashboard adhering to senior quantitative trading interface design standards.

---

## Strategy Overview

This implementation covers all levels of the internship challenge:
- **Level 1 – Naked BTC Short Straddle**:
  - Connects to Bybit Testnet / Demo.
  - Automatically queries underlying BTC reference price ($S_{\text{ref}}$) and active BTC option contracts.
  - Determines the nearest At-The-Money (ATM) strike.
  - Selects matching Call and Put contracts with identical underlying, strike, and expiry.
  - Submits Sell orders for both options.
  - Tracks order acknowledgments, executions, fills, and open positions.
  - No automatic stop-loss or take-profit logic.
- **Level 2 – Automated Risk Management**:
  - Computes combined entry premium ($V_{\text{entry}} = P_{\text{entry, call}} + P_{\text{entry, put}}$).
  - Continuously monitors combined buyback premium using **ask-side quotes**:
    $$V_{\text{buyback}} = P_{\text{ask, call}} + P_{\text{ask, put}}$$
  - **Stop-Loss (+1%)**: If buyback premium increases by 1% from entry ($V_{\text{buyback}} \ge 1.01 \times V_{\text{entry}}$), automatically closes both positions.
  - **Take-Profit (-2%)**: If buyback premium decreases by 2% from entry ($V_{\text{buyback}} \le 0.98 \times V_{\text{entry}}$), automatically locks in profit and closes both positions.
  - Guards against duplicate exit attempts (idempotent state machine).
  - Legging risk handler safely unwinds unhedged single-leg executions.

---

## User Interface Design System (Light Theme)

The application runs on **one clean desktop page** structured into 5 logical sections:
1. **Exchange & Engine Connection**: Real-time status indicators (`BYBIT TESTNET — CONNECTED`, `PUBLIC MARKET DATA — CONNECTED`, `PRIVATE API — AUTHENTICATED`, `NAUTILUSTRADER — RUNNING`) and connection refresh action.
2. **BTC Options Discovery & ATM Selection**: Live BTC reference price, target expiry selector, available contracts count, calculated ATM strike, and interactive horizontal strike selector chips highlighting the ATM strike.
3. **Selected Call & Put Option Pair**: Balanced side-by-side cards with symbols, strikes, bids, asks, bid/ask quantities, and a semantic `PAIR VALIDATED` banner.
4. **Strategy Configuration & Active Monitor**:
   - Level 1 Naked Straddle vs Level 2 Automated Risk Management execution triggers.
   - **Pre-flight Confirmation Modal**: Displays leg details, quantities, strike, expiry, and estimated entry premium before order submission.
   - **Active Strategy Monitor**: Real-time strategy state (`IDLE`, `VALIDATING`, `ENTRY_PENDING`, `OPEN`, `EXIT_TRIGGERED`, `CLOSED`), Combined Entry, Live Buyback Cost, Unrealized PnL ($ / %), and risk thresholds.
5. **Orders & Positions Tabs**:
   - **Orders Tab**: Time, Contract, Side, Quantity, Filled Qty, Avg Fill Price, Status.
   - **Positions Tab**: Contract, Side, Quantity, Avg Entry, Mark Price, Unrealized PnL, with unhedged exposure warnings.
6. **Execution Logs**: Structured activity feed (`Time | Event | Description | Status`) with semantic status badges.

---

## Quick Start

### 1. Launch the Web Trading Dashboard
```powershell
python run_dashboard.py --port 8000 --env DEMO
```
Open **`http://localhost:8000`** in your browser.

### 2. Run via Standalone CLI
```powershell
# Run Level 1 Naked Straddle
python run_straddle.py --level 1 --qty 0.01 --env DEMO

# Run Level 2 Automated 1% SL / 2% TP Straddle
python run_straddle.py --level 2 --qty 0.01 --env DEMO
```

### 3. Run the Test Suite
```powershell
python -m pytest -v tests
```

---

## Repository

- **GitHub Repository**: [https://github.com/Amankum25/mernplus-intern-assignment](https://github.com/Amankum25/mernplus-intern-assignment)
- **Clone URL**: `git clone https://github.com/Amankum25/mernplus-intern-assignment.git`

See the full written walkthrough and presentation guide in [WALKTHROUGH.md](WALKTHROUGH.md) or open the **"Interview Walkthrough"** tab in the web dashboard!

