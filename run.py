#!/usr/bin/env python3
"""Trading Bot CLI — backtest, compare, and paper trade."""

import argparse
import os
import sys
import yaml
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.data import fetch_data, fetch_pair
from core.engine import BacktestEngine
from core.risk import RiskManager
from core.broker import AlpacaBroker
from core.rebalance import RebalanceEngine, run_frequency_comparison
from core.live_rebalance import run_live_rebalance, print_rebalance_result
from strategies import REGISTRY


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        config = yaml.safe_load(f)
    # Env var overrides for secrets
    if os.environ.get("APCA_API_KEY_ID"):
        config.setdefault("alpaca", {})["api_key"] = os.environ["APCA_API_KEY_ID"]
    if os.environ.get("APCA_API_SECRET_KEY"):
        config.setdefault("alpaca", {})["secret_key"] = os.environ["APCA_API_SECRET_KEY"]
    return config


def run_backtest(strategy_name: str, config: dict) -> dict:
    """Run a single strategy backtest and return results."""
    if strategy_name not in REGISTRY:
        print(f"Unknown strategy: {strategy_name}")
        print(f"Available: {', '.join(REGISTRY.keys())}")
        sys.exit(1)

    strat_config = config["strategies"].get(strategy_name, {})
    strategy = REGISTRY[strategy_name](strat_config)
    risk = RiskManager(config["risk"])

    data_cfg = config["data"]
    period = data_cfg.get("default_period", "6mo")
    interval = data_cfg.get("interval", "1h")
    cache_dir = data_cfg.get("cache_dir", "data/")
    cache_max_hours = data_cfg.get("cache_max_hours", 1)

    # Fetch data
    if strategy_name == "pairs":
        symbol_a = strat_config.get("symbol_a", "XRP/USD")
        symbol_b = strat_config.get("symbol_b", "BTC/USD")
        df = fetch_pair(symbol_a, symbol_b, period, interval, cache_dir, cache_max_hours)
    else:
        symbol = strat_config.get("symbol", "XRP/USD")
        df = fetch_data(symbol, period, interval, cache_dir, cache_max_hours)

    # Determine initial capital
    budget = 250.0
    for asset in config.get("assets", []):
        if asset.get("symbol") == strat_config.get("symbol", "XRP/USD"):
            budget = asset.get("budget", asset.get("budget_monthly", 250.0))
            break

    engine = BacktestEngine(strategy, risk, initial_capital=budget)
    return engine.run(df)


def print_results(results: dict):
    """Pretty print backtest results."""
    print(f"\n{'='*50}")
    print(f"  Strategy: {results['strategy']}")
    print(f"  Params:   {results['params']}")
    print(f"{'='*50}")
    print(f"  Initial Capital:  ${results['initial_capital']:.2f}")
    print(f"  Final Equity:     ${results['final_equity']:.2f}")
    print(f"  Total Return:     {results['total_return_pct']:+.2f}%")
    print(f"  Total Trades:     {results['total_trades']}")
    print(f"  Winning Trades:   {results['winning_trades']}")
    print(f"  Losing Trades:    {results['losing_trades']}")
    print(f"  Win Rate:         {results['win_rate_pct']:.1f}%")
    print(f"  Max Drawdown:     {results['max_drawdown_pct']:.2f}%")
    print(f"  Sharpe Ratio:     {results['sharpe_ratio']:.2f}")
    print(f"{'='*50}\n")

    # Show last 5 trades
    if results["trades"]:
        print("  Recent trades:")
        for t in results["trades"][-5:]:
            pnl_str = f"  P&L: ${t['pnl']:+.2f}" if t["pnl"] != 0 else ""
            print(f"    {t['date']} | {t['action']:12s} | "
                  f"${t['price']:.2f} x {t['shares']:.4f}{pnl_str}")
        print()


def compare_strategies(config: dict):
    """Run all strategies and print side-by-side comparison."""
    print("\n" + "="*70)
    print("  STRATEGY COMPARISON")
    print("="*70)

    all_results = []
    for name in REGISTRY:
        print(f"\nRunning {name}...")
        results = run_backtest(name, config)
        all_results.append(results)

    # Comparison table
    print(f"\n{'Strategy':<16} {'Return':>10} {'Trades':>8} {'Win%':>8} {'Drawdown':>10} {'Sharpe':>8}")
    print("-" * 62)
    for r in all_results:
        print(f"{r['strategy']:<16} {r['total_return_pct']:>+9.2f}% {r['total_trades']:>8} "
              f"{r['win_rate_pct']:>7.1f}% {r['max_drawdown_pct']:>9.2f}% {r['sharpe_ratio']:>8.2f}")

    # Winner
    best = max(all_results, key=lambda r: r["sharpe_ratio"])
    print(f"\n  Best Sharpe ratio: {best['strategy']} ({best['sharpe_ratio']:.2f})")
    best_return = max(all_results, key=lambda r: r["total_return_pct"])
    print(f"  Best return:       {best_return['strategy']} ({best_return['total_return_pct']:+.2f}%)")
    print()


def show_account(config: dict):
    """Show Alpaca account info and open positions."""
    broker = AlpacaBroker(paper=config["alpaca"].get("paper", True))
    acct = broker.get_account()

    print(f"\n{'='*50}")
    print(f"  ALPACA PAPER ACCOUNT")
    print(f"{'='*50}")
    print(f"  Status:       {acct['status']}")
    print(f"  Equity:       ${acct['equity']:,.2f}")
    print(f"  Cash:         ${acct['cash']:,.2f}")
    print(f"  Buying Power: ${acct['buying_power']:,.2f}")
    print(f"{'='*50}")

    positions = broker.get_positions()
    if positions:
        print(f"\n  Open Positions:")
        for p in positions:
            print(f"    {p['symbol']:6s} | {p['qty']:.4f} shares | "
                  f"Entry: ${p['avg_entry']:.2f} | Now: ${p['current_price']:.2f} | "
                  f"P&L: ${p['unrealized_pnl']:+.2f} ({p['unrealized_pnl_pct']:+.1f}%)")
    else:
        print("\n  No open positions.")

    orders = broker.get_orders("open")
    if orders:
        print(f"\n  Open Orders:")
        for o in orders:
            print(f"    {o['symbol']} | {o['side']} | qty={o['qty']} | status={o['status']}")
    print()


def run_paper_signal(strategy_name: str, config: dict):
    """Run one signal check against live data and execute if actionable."""
    if strategy_name not in REGISTRY:
        print(f"Unknown strategy: {strategy_name}")
        sys.exit(1)

    broker = AlpacaBroker(paper=config["alpaca"].get("paper", True))
    acct = broker.get_account()
    risk = RiskManager(config["risk"])

    strat_config = config["strategies"].get(strategy_name, {})
    strategy = REGISTRY[strategy_name](strat_config)

    data_cfg = config["data"]
    symbol = strat_config.get("symbol", "XRP/USD")
    interval = data_cfg.get("interval", "1h")
    cache_dir = data_cfg.get("cache_dir", "data/")
    cache_max_hours = data_cfg.get("cache_max_hours", 1)

    # Fetch recent data for signal generation
    if strategy_name == "pairs":
        df = fetch_pair(strat_config["symbol_a"], strat_config["symbol_b"],
                        period="6mo", interval=interval,
                        cache_dir=cache_dir, cache_max_hours=cache_max_hours)
    else:
        df = fetch_data(symbol, period="6mo", interval=interval,
                        cache_dir=cache_dir, cache_max_hours=cache_max_hours)

    signals = strategy.generate_signals(df)
    latest_signal = signals["signal"].iloc[-1]
    latest_price = signals["Close"].iloc[-1]

    print(f"\n  Strategy: {strategy_name}")
    print(f"  Symbol:   {symbol}")
    print(f"  Price:    ${latest_price:.2f}")
    print(f"  Signal:   {'BUY' if latest_signal == 1 else 'SELL' if latest_signal == -1 else 'HOLD'}")
    print(f"  Equity:   ${acct['equity']:,.2f}")

    if risk.check_drawdown(acct["equity"]):
        print("  [KILL SWITCH] Max drawdown exceeded. No trades.")
        return

    positions = broker.get_positions()
    holding = any(p["symbol"] == symbol for p in positions)

    if latest_signal == 1 and not holding:
        dollars = risk.position_size(acct["equity"], latest_price)
        dollars = min(dollars, acct["cash"])
        if dollars >= 1.0:
            print(f"  -> Buying ${dollars:.2f} of {symbol}...")
            order = broker.buy(symbol, notional=round(dollars, 2))
            print(f"  -> Order placed: {order['id']} ({order['status']})")
        else:
            print("  -> Insufficient funds for trade.")

    elif latest_signal == -1 and holding:
        print(f"  -> Selling all {symbol}...")
        order = broker.close_position(symbol)
        print(f"  -> Order placed: {order['id']} ({order['status']})")

    else:
        print("  -> No action needed.")
    print()


def run_rebalance(config: dict, args):
    """Run portfolio rebalancing at multiple frequencies and compare."""
    rb_cfg = config.get("rebalance", {})
    allocations = rb_cfg.get("allocations", {
        "PAXG/USD": 0.25, "BTC/USD": 0.25, "XRP/USD": 0.25, "CASH": 0.25
    })
    capital = args.capital or rb_cfg.get("initial_capital", 1000.0)
    dca = args.dca or rb_cfg.get("monthly_dca", 100.0)
    period = args.period or rb_cfg.get("period", "2y")
    cache_dir = config["data"].get("cache_dir", "data/")

    print(f"\n{'='*80}")
    print(f"  PORTFOLIO REBALANCING — Frequency Comparison")
    print(f"{'='*80}")
    print(f"  Initial capital: ${capital:,.2f}")
    print(f"  Monthly DCA:     ${dca:,.2f}")
    print(f"  Period:          {period}")
    print(f"  Allocations:")
    for asset, pct in allocations.items():
        print(f"    {asset:<12s}  {pct*100:.0f}%")
    print()

    results = run_frequency_comparison(allocations, capital, dca, period, cache_dir)

    # Results table
    print(f"  {'Frequency':<16} {'Invested':>10} {'Final':>10} {'Return':>9} {'vs B&H':>9} "
          f"{'Rebal#':>7} {'MaxDD':>8}")
    print("  " + "-" * 72)
    for r in results:
        print(f"  {r['frequency_label']:<16} "
              f"${r['total_invested']:>9,.2f} "
              f"${r['final_value']:>9,.2f} "
              f"{r['total_return_pct']:>+8.2f}% "
              f"{r['beat_buyhold_pct']:>+8.2f}% "
              f"{r['rebalance_count']:>7} "
              f"{r['max_drawdown_pct']:>7.2f}%")

    # Buy & hold reference
    bh = results[0]
    print(f"\n  Buy & Hold:      ${bh['total_invested']:>9,.2f} ${bh['buyhold_final']:>9,.2f} "
          f"{bh['buyhold_return_pct']:>+8.2f}%")

    # Best
    best = max(results, key=lambda r: r["final_value"])
    print(f"\n  Winner: {best['frequency_label']} "
          f"(${best['final_value']:,.2f}, {best['total_return_pct']:+.2f}%, "
          f"beats B&H by {best['beat_buyhold_pct']:+.2f}%)")
    print()


def main():
    parser = argparse.ArgumentParser(description="Trading Bot")
    sub = parser.add_subparsers(dest="command")

    # Backtest
    bt = sub.add_parser("backtest", help="Run backtest on historical data")
    bt.add_argument("--strategy", "-s", default="ma_crossover",
                    help=f"Strategy name ({', '.join(REGISTRY.keys())})")
    bt.add_argument("--compare", "-c", action="store_true",
                    help="Compare all strategies side by side")

    # Paper trading
    pp = sub.add_parser("paper", help="Run paper trading (requires Alpaca API keys)")
    pp.add_argument("--strategy", "-s", default=None,
                    help="Strategy to use (default: from config)")

    # Rebalance comparison
    rb = sub.add_parser("rebalance", help="Test portfolio rebalancing at different frequencies")
    rb.add_argument("--capital", "-c", type=float, default=None,
                    help="Initial capital (default: from config)")
    rb.add_argument("--dca", "-d", type=float, default=None,
                    help="Monthly DCA amount (default: from config)")
    rb.add_argument("--period", "-p", default=None,
                    help="Backtest period (default: from config)")

    # Live rebalance
    lr = sub.add_parser("rebalance-live", help="Run adaptive rebalancing on Alpaca (paper)")
    lr.add_argument("--capital", "-c", type=float, default=None,
                    help="Capital to deploy on initial buy (default: from config)")
    lr.add_argument("--dry-run", action="store_true", default=False,
                    help="Show what would happen without placing orders")
    lr.add_argument("--force", action="store_true", default=False,
                    help="Force rebalance regardless of schedule")

    # Account info
    sub.add_parser("account", help="Show Alpaca account info and positions")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return

    # Change to project dir for relative paths
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv()
    config = load_config()

    if args.command == "backtest":
        if args.compare:
            compare_strategies(config)
        else:
            results = run_backtest(args.strategy, config)
            print_results(results)

    elif args.command == "rebalance":
        run_rebalance(config, args)

    elif args.command == "rebalance-live":
        rb_cfg = config.get("rebalance", {})
        allocations = rb_cfg.get("allocations", {})
        capital = args.capital or rb_cfg.get("initial_capital", 1000.0)
        dca = rb_cfg.get("monthly_dca", 100.0)
        broker = AlpacaBroker(paper=config["alpaca"].get("paper", True))

        result = run_live_rebalance(
            allocations=allocations,
            capital_to_deploy=capital,
            monthly_dca=dca,
            broker=broker,
            dry_run=args.dry_run,
            force=args.force,
        )
        print_rebalance_result(result)

    elif args.command == "account":
        show_account(config)

    elif args.command == "paper":
        strategy_name = args.strategy or config["strategies"]["active"]
        run_paper_signal(strategy_name, config)


if __name__ == "__main__":
    main()
