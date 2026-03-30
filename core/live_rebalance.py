"""Live Adaptive Rebalancer for Alpaca paper/live trading.

Reads current positions, compares to target allocations, and places
orders to rebalance. Uses SPY 200-day SMA for bull/bear detection
to determine rebalancing frequency.

State (last rebalance date, last DCA month) persisted in a JSON file.
"""

import json
import os
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from core.broker import AlpacaBroker, is_crypto


STATE_FILE = "data/rebalance_state.json"


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def get_macro_regime() -> dict:
    """Fetch SPY and compute 200-day SMA to determine bull/bear."""
    df = yf.download("SPY", period="1y", interval="1d", progress=False)
    if len(df) < 200:
        return {"regime": "BULL", "spy_price": 0, "sma_200": 0, "gap_pct": 0}

    sma_200 = float(df["Close"].rolling(200).mean().iloc[-1].iloc[0])
    spy_price = float(df["Close"].iloc[-1].iloc[0])
    gap_pct = (spy_price - sma_200) / sma_200 * 100

    return {
        "regime": "BULL" if spy_price > sma_200 else "BEAR",
        "spy_price": round(spy_price, 2),
        "sma_200": round(sma_200, 2),
        "gap_pct": round(gap_pct, 2),
    }


def alpaca_symbol(symbol: str) -> str:
    """Convert config symbol to Alpaca format. Alpaca crypto uses '/' (BTC/USD)."""
    return symbol


def position_symbol(symbol: str) -> str:
    """Convert config symbol to Alpaca position format (no slash for crypto)."""
    return symbol.replace("/", "")


def run_live_rebalance(allocations: dict, capital_to_deploy: float,
                       monthly_dca: float, broker: AlpacaBroker,
                       dry_run: bool = True, force: bool = False,
                       bull_days: int = 126, bear_days: int = 21) -> dict:
    """
    Run one rebalance cycle.

    capital_to_deploy: how much of the account to use (0 = use current positions only)
    dry_run: if True, only print what would happen, don't place orders
    force: if True, rebalance regardless of schedule
    """
    state = load_state()
    asset_symbols = [s for s in allocations if s != "CASH"]

    # 1. Macro regime
    macro = get_macro_regime()
    rebalance_interval = bear_days if macro["regime"] == "BEAR" else bull_days

    # 2. Check if rebalance is due
    last_rebalance = state.get("last_rebalance_date")
    if last_rebalance:
        last_dt = datetime.fromisoformat(last_rebalance)
        trading_days_since = (datetime.now() - last_dt).days * 5 / 7  # approximate
        if trading_days_since < rebalance_interval and not force:
            next_in = int(rebalance_interval - trading_days_since)
            return {
                "action": "SKIP",
                "reason": f"Last rebalance {last_dt.strftime('%Y-%m-%d')}. "
                          f"Next in ~{next_in} trading days ({macro['regime']} mode).",
                "macro": macro,
            }

    # 3. Check for pending orders — skip if any exist
    open_orders = broker.get_orders("open")
    if open_orders and not force:
        return {
            "action": "SKIP",
            "reason": f"{len(open_orders)} pending orders exist. "
                      f"Wait for them to fill or cancel first.",
            "macro": macro,
        }

    # 4. Get account and positions
    acct = broker.get_account()
    positions = broker.get_positions()
    pos_map = {p["symbol"]: p for p in positions}

    # 4. Get latest prices for all assets
    prices = {}
    for symbol in asset_symbols:
        try:
            prices[symbol] = broker.get_latest_price(symbol)
        except Exception as e:
            print(f"  [WARN] Could not get price for {symbol}: {e}")
            prices[symbol] = None

    # 5. Calculate current portfolio value
    current_holdings = {}
    for symbol in asset_symbols:
        pos_sym = position_symbol(symbol)
        if pos_sym in pos_map:
            current_holdings[symbol] = {
                "qty": pos_map[pos_sym]["qty"],
                "value": pos_map[pos_sym]["market_value"],
                "price": prices.get(symbol, pos_map[pos_sym]["current_price"]),
            }
        else:
            current_holdings[symbol] = {
                "qty": 0,
                "value": 0,
                "price": prices.get(symbol, 0),
            }

    invested_value = sum(h["value"] for h in current_holdings.values())
    cash_available = float(acct["cash"])

    # If first time (no positions), deploy initial capital
    is_initial = invested_value < 10 and capital_to_deploy > 0
    if is_initial:
        portfolio_value = capital_to_deploy
    else:
        portfolio_value = invested_value + (cash_available * allocations.get("CASH", 0))

    # 6. Monthly DCA check
    now = datetime.now()
    month_key = f"{now.year}-{now.month:02d}"
    last_dca_month = state.get("last_dca_month", "")
    dca_applied = False
    if month_key != last_dca_month and not is_initial:
        portfolio_value += monthly_dca
        dca_applied = True

    # 7. Calculate target vs current, generate orders
    orders = []
    for symbol in asset_symbols:
        target_value = portfolio_value * allocations[symbol]
        current_value = current_holdings[symbol]["value"]
        price = current_holdings[symbol]["price"]

        if price is None or price <= 0:
            continue

        diff = target_value - current_value

        if abs(diff) < 1.0:  # skip dust (<$1)
            continue

        if diff > 0:
            orders.append({
                "symbol": symbol,
                "side": "BUY",
                "notional": round(diff, 2),
                "reason": f"Under by ${diff:.2f} (target ${target_value:.2f}, have ${current_value:.2f})",
            })
        else:
            # For sells, calculate qty to sell
            qty_to_sell = abs(diff) / price
            if current_holdings[symbol]["qty"] > 0 and qty_to_sell > 0.0001:
                qty_to_sell = min(qty_to_sell, current_holdings[symbol]["qty"])
                orders.append({
                    "symbol": symbol,
                    "side": "SELL",
                    "qty": round(qty_to_sell, 6),
                    "notional": round(abs(diff), 2),
                    "reason": f"Over by ${abs(diff):.2f} (target ${target_value:.2f}, have ${current_value:.2f})",
                })

    # 8. Execute orders (sells first, then buys)
    executed = []
    sells = [o for o in orders if o["side"] == "SELL"]
    buys = [o for o in orders if o["side"] == "BUY"]

    for order in sells + buys:
        symbol = order["symbol"]
        if dry_run:
            executed.append({**order, "status": "DRY_RUN"})
            continue

        try:
            if order["side"] == "BUY":
                notional = order["notional"]
                if notional > cash_available:
                    notional = cash_available
                if notional < 1.0:
                    executed.append({**order, "status": "SKIPPED (insufficient cash)"})
                    continue
                result = broker.buy(alpaca_symbol(symbol), notional=notional)
                cash_available -= notional
            else:
                result = broker.sell(alpaca_symbol(symbol), qty=order["qty"])
                cash_available += order["notional"]

            executed.append({**order, "status": result["status"], "order_id": result["id"]})
        except Exception as e:
            executed.append({**order, "status": f"ERROR: {e}"})

    # 9. Update state
    if not dry_run and executed:
        state["last_rebalance_date"] = now.isoformat()
        if dca_applied:
            state["last_dca_month"] = month_key
        state["last_regime"] = macro["regime"]
        save_state(state)

    return {
        "action": "INITIAL_BUY" if is_initial else "REBALANCE",
        "macro": macro,
        "portfolio_value": round(portfolio_value, 2),
        "dca_applied": dca_applied,
        "orders": executed,
        "dry_run": dry_run,
    }


def print_rebalance_result(result: dict):
    """Pretty-print rebalance results."""
    print(f"\n{'='*60}")

    if result["action"] == "SKIP":
        print(f"  REBALANCE SKIPPED")
        print(f"  {result['reason']}")
        macro = result["macro"]
        print(f"  Market: {macro['regime']} (SPY ${macro['spy_price']} vs "
              f"SMA200 ${macro['sma_200']}, {macro['gap_pct']:+.1f}%)")
        print(f"{'='*60}\n")
        return

    macro = result["macro"]
    mode = "DRY RUN" if result["dry_run"] else "LIVE"
    print(f"  {result['action']} — {mode}")
    print(f"{'='*60}")
    print(f"  Market:     {macro['regime']} (SPY ${macro['spy_price']} vs "
          f"SMA200 ${macro['sma_200']}, {macro['gap_pct']:+.1f}%)")
    print(f"  Portfolio:  ${result['portfolio_value']:,.2f}")
    if result["dca_applied"]:
        print(f"  DCA:        Applied this month")

    orders = result["orders"]
    if orders:
        print(f"\n  {'Symbol':<12} {'Side':<6} {'Amount':>10} {'Status':<20}")
        print(f"  {'-'*50}")
        for o in orders:
            amt = f"${o['notional']:.2f}"
            print(f"  {o['symbol']:<12} {o['side']:<6} {amt:>10} {o['status']:<20}")
    else:
        print(f"\n  Portfolio is balanced — no trades needed.")

    print(f"{'='*60}\n")
