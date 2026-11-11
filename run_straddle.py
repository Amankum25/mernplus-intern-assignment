"""
Official NautilusTrader BTC Options Straddle Live Runner.

Uses native NautilusTrader TradingNode, BybitDataClient, and BybitExecutionClient
adapters to trade an automated ATM Bitcoin Short Straddle on Bybit Testnet/Demo/Mainnet.

Usage:
    python run_straddle.py --level 2 --qty 0.01 --env TESTNET
"""

import argparse
from decimal import Decimal
import os
import sys
from dotenv import load_dotenv

from nautilus_trader.adapters.bybit import (
    BybitDataClientConfig,
    BybitExecClientConfig,
    BybitEnvironment,
    BybitProductType,
)
from nautilus_trader.adapters.bybit.factories import (
    BybitLiveDataClientFactory,
    BybitLiveExecClientFactory,
    get_cached_bybit_http_client,
    get_cached_bybit_instrument_provider,
)
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId, TraderId

from straddle.config import BTCShortStraddleConfig
from straddle.strategy import BTCShortStraddleStrategy
from straddle.strike_selector import BybitStrikeSelector

load_dotenv()


def main():
    parser = argparse.ArgumentParser(description="NautilusTrader BTC Options Straddle Live Runner")
    parser.add_argument("--level", type=int, default=2, choices=[1, 2], help="1: Naked Straddle, 2: Automated 1%% SL / 2%% TP")
    parser.add_argument("--qty", type=float, default=float(os.getenv("DEFAULT_TRADE_QTY", "0.01")), help="Trade size in BTC")
    parser.add_argument("--env", type=str, default=os.getenv("BYBIT_ENVIRONMENT", "TESTNET").upper(), choices=["DEMO", "TESTNET", "MAINNET"], help="Bybit environment")

    args = parser.parse_args()

    print("\n" + "=" * 75)
    print("   ⚡ NAUTILUSTRADER BTC SHORT STRADDLE — OFFICIAL BYBIT LIVE RUNNER")
    print(f"   Mode         : Level {args.level} ({'Naked Straddle' if args.level == 1 else 'Automated 1% SL / 2% TP'})")
    print(f"   Environment  : Bybit {args.env}")
    print(f"   Order Size   : {args.qty} BTC per leg")
    print("=" * 75 + "\n")

    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")

    # Resolve environment enum
    env_map = {
        "TESTNET": BybitEnvironment.TESTNET,
        "DEMO": BybitEnvironment.DEMO,
        "MAINNET": BybitEnvironment.MAINNET,
    }
    bybit_env = env_map.get(args.env, BybitEnvironment.TESTNET)

    # 1. Discover current BTC Spot price & nearest ATM Call/Put contracts
    print("[1/4] Discovering live BTC options chain and nearest ATM strike...")
    selector = BybitStrikeSelector()
    pair = selector.select_atm_straddle()

    print(f"      -> Current BTC Spot Price: ${pair.btc_price:,.2f}")
    print(f"      -> Target Expiration     : {pair.expiry_str}")
    print(f"      -> Selected ATM Strike   : ${pair.atm_strike:,.2f}")
    print(f"      -> Selected Call Leg     : {pair.call_contract.symbol}")
    print(f"      -> Selected Put Leg      : {pair.put_contract.symbol}\n")

    call_id = InstrumentId.from_str(pair.call_contract.instrument_id)
    put_id = InstrumentId.from_str(pair.put_contract.instrument_id)

    # 2. Configure native Nautilus TradingNode with Bybit Data and Execution Clients
    print("[2/4] Configuring Nautilus TradingNode with native Bybit adapters...")
    from nautilus_trader.config import InstrumentProviderConfig
    inst_cfg = InstrumentProviderConfig(load_ids=(call_id, put_id))

    node_config = TradingNodeConfig(
        trader_id=TraderId("BYBIT-TRADER-001"),
        data_clients={
            "BYBIT": BybitDataClientConfig(
                api_key=api_key,
                api_secret=api_secret,
                product_types=(BybitProductType.OPTION,),
                environment=bybit_env,
                instrument_provider=inst_cfg,
            ),
        },
        exec_clients={
            "BYBIT": BybitExecClientConfig(
                api_key=api_key,
                api_secret=api_secret,
                product_types=(BybitProductType.OPTION,),
                environment=bybit_env,
                instrument_provider=inst_cfg,
            ),
        },
    )

    node = TradingNode(config=node_config)
    node.add_data_client_factory("BYBIT", BybitLiveDataClientFactory)
    node.add_exec_client_factory("BYBIT", BybitLiveExecClientFactory)

    # 3. Build Node (initializes internal engines and adapter handshakes)
    print("[3/4] Building TradingNode (reconciling cache, order emulator & venue)...")
    node.build()

    # Pre-populate selected option instruments into cache
    client = get_cached_bybit_http_client(environment=bybit_env, api_key=api_key, api_secret=api_secret)
    provider = get_cached_bybit_instrument_provider(client=client, product_types=(BybitProductType.OPTION,))
    provider.load_all()
    for inst in provider.list_all():
        if inst.id in (call_id, put_id):
            node.cache.add_instrument(inst)

    # 4. Instantiate strategy and add to Trader
    strategy_config = BTCShortStraddleConfig(
        call_instrument_id=call_id,
        put_instrument_id=put_id,
        trade_size=Decimal(str(args.qty)),
        level=args.level,
        stop_loss_pct=Decimal("0.01"),
        take_profit_pct=Decimal("0.02"),
    )
    strategy = BTCShortStraddleStrategy(config=strategy_config)
    node.trader.add_strategy(strategy)

    print("[4/4] Starting NautilusTrader Engine...")
    print(f"      -> Entering short straddle: Sell {args.qty} Call & {args.qty} Put")
    print(f"      -> SL (+1%): Buyback >= Entry * 1.01 | TP (-2%): Buyback <= Entry * 0.98")
    print("      -> Press Ctrl+C to stop.\n")

    try:
        node.run()
    except KeyboardInterrupt:
        print("\nStopping TradingNode...")
        node.stop()
        node.dispose()
        print("Shutdown complete.")


if __name__ == "__main__":
    main()
