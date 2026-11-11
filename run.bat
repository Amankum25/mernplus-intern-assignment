@echo off
REM ===================================================================
REM BTC Options Straddle - All-In-One Launcher
REM Usage:
REM   run             (Runs tests, queries market, opens dashboard)
REM   run --test      (Runs tests only)
REM   run --cli       (Runs terminal CLI strategy)
REM   run --web       (Starts dashboard directly)
REM ===================================================================

cd /d "%~dp0"

REM Prefer virtual environment python if present
if exist ".\.venv\Scripts\python.exe" (
    ".\.venv\Scripts\python.exe" run.py %*
) else (
    python run.py %*
)

