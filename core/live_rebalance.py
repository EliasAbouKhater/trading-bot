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
MIN_CRYPTO_NOTIONAL = 10.0  # Alpaca hard minimum for crypto notional orders

# Round-trip fee estimates by asset class (entry + eventual exit on Alpaca live).
# Stock/ETF: 0% commission + ~0.05% spread each way  → ~0.10% total
# Crypto:    0.25% Alpaca fee each way + ~0.05% spread → ~0.60% total
# Source: Alpaca fee schedule (as of 2026)
ROUND_TRIP_FEE = {
    "stock":  0.0010,
    "crypto": 0.0060,
}
# A trade is only placed when: drift_amount > portfolio_value × fee_rate × FEE_COVERAGE
# Coverage = 2 means "the drift must cover the round-trip fee twice over" — a safety margin.
# This prevents marginal trades where fees eat most of the rebalancing benefit.
FEE_COVERAGE = 2.0


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def get_macro_regime(last_known_regime: str = "BULL") -> dict:
    """Fetch SPY and compute 200-day SMA to determine bull/bear.

    Falls back to last_known_regime on network/data failure so a transient
    DNS blip doesn't crash the cron or force an incorrect regime.
    """
    try:
        df = yf.download("SPY", period="1y", interval="1d", progress=False)
        if len(df) < 200:
            return {"regime": last_known_regime, "spy_price": 0, "sma_200": 0,
                    "gap_pct": 0, "fallback": True}

        sma_200 = float(df["Close"].rolling(200).mean().iloc[-1].iloc[0])
        spy_price = float(df["Close"].iloc[-1].iloc[0])
        gap_pct = (spy_price - sma_200) / sma_200 * 100

        return {
            "regime": "BULL" if spy_price > sma_200 else "BEAR",
            "spy_price": round(spy_price, 2),
            "sma_200": round(sma_200, 2),
            "gap_pct": round(gap_pct, 2),
        }
    except Exception:
        return {"regime": last_known_regime, "spy_price": 0, "sma_200": 0,
                "gap_pct": 0, "fallback": True}


def alpaca_symbol(symbol: str) -> str:
    """Convert config symbol to Alpaca format. Alpaca crypto uses '/' (BTC/USD)."""
    return symbol


def position_symbol(symbol: str) -> str:
    """Convert config symbol to Alpaca position format (no slash for crypto)."""
    return symbol.replace("/", "")


STALE_ORDER_DAYS = 2   # cancel open orders older than this before rebalancing
POOL_THRESHOLD    = 20.0  # consolidate all crypto buys into one if total < this


def run_live_rebalance(allocations: dict, capital_to_deploy: float,
                       monthly_dca: float, broker: AlpacaBroker,
                       dry_run: bool = True, force: bool = False,
                       bull_days: int = 126, bear_days: int = 21) -> dict:
    """
    Run one rebalance cycle.

    Architecture: DECIDE → RECORD → EXECUTE
      1. Compute what needs rebalancing (decision).
      2. Persist the decision immediately (state saved before any order is placed).
      3. Execute orders. Errors are collected and returned — they do NOT roll back
         the recorded decision. The caller is responsible for alerting on errors.

    capital_to_deploy: how much of the account to use on first run (no positions yet)
    dry_run: if True, compute orders but place nothing and do not save state
    force:   if True, skip the time-interval check (drift-triggered runs)
    """
    state = load_state()
    asset_symbols = [s for s in allocations if s != "CASH"]
    now = datetime.now()

    # ── 1. Macro regime ────────────────────────────────────────────────────────
    macro = get_macro_regime(last_known_regime=state.get("last_regime", "BULL"))
    rebalance_interval = bear_days if macro["regime"] == "BEAR" else bull_days

    # ── 2. Time-based skip ─────────────────────────────────────────────────────
    last_rebalance = state.get("last_rebalance_date")
    if last_rebalance:
        last_dt = datetime.fromisoformat(last_rebalance)
        trading_days_since = (now - last_dt).days * 5 / 7
        if trading_days_since < rebalance_interval and not force:
            next_in = int(rebalance_interval - trading_days_since)
            return {
                "action": "SKIP",
                "reason": f"Last rebalance {last_dt.strftime('%Y-%m-%d')}. "
                          f"Next in ~{next_in} trading days ({macro['regime']} mode).",
                "macro": macro,
            }

    # ── 3. Stale order cancellation (M-02) ────────────────────────────────────
    # Cancel open orders older than STALE_ORDER_DAYS so they don't block
    # rebalancing indefinitely. Fresh open orders still cause a SKIP — fills
    # are expected to arrive and will be reflected next cron cycle.
    open_orders = broker.get_orders("open")
    stale_cancelled = []
    if not dry_run and open_orders:
        for o in open_orders:
            created_str = o["created_at"].split("+")[0].split(".")[0]
            created = datetime.fromisoformat(created_str)
            if (now - created).days >= STALE_ORDER_DAYS:
                try:
                    broker.cancel_order(o["id"])
                    stale_cancelled.append(o)
                except Exception:
                    pass
        if stale_cancelled:
            open_orders = broker.get_orders("open")  # refresh after cancellations

    if open_orders and not force:
        return {
            "action": "SKIP",
            "reason": f"{len(open_orders)} open order(s) pending fills.",
            "macro": macro,
            "stale_cancelled": stale_cancelled,
        }

    # ── 4. Snapshot: positions, prices, portfolio value ───────────────────────
    acct      = broker.get_account()
    positions = broker.get_positions()
    pos_map   = {p["symbol"]: p for p in positions}

    prices = {}
    for symbol in asset_symbols:
        try:
            prices[symbol] = broker.get_latest_price(symbol)
        except Exception as e:
            print(f"  [WARN] Could not get price for {symbol}: {e}")
            prices[symbol] = None

    current_holdings = {}
    for symbol in asset_symbols:
        pos_sym = position_symbol(symbol)
        if pos_sym in pos_map:
            current_holdings[symbol] = {
                "qty":   pos_map[pos_sym]["qty"],
                "value": pos_map[pos_sym]["market_value"],
                "price": prices.get(symbol, pos_map[pos_sym]["current_price"]),
            }
        else:
            current_holdings[symbol] = {"qty": 0, "value": 0, "price": prices.get(symbol, 0)}

    invested_value = sum(h["value"] for h in current_holdings.values())

    is_initial = invested_value < 10 and capital_to_deploy > 0
    if is_initial:
        portfolio_value = capital_to_deploy
    else:
        portfolio_value = invested_value + (float(acct["cash"]) * allocations.get("CASH", 0))

    # ── 5. Monthly DCA ────────────────────────────────────────────────────────
    month_key     = f"{now.year}-{now.month:02d}"
    dca_applied   = False
    if month_key != state.get("last_dca_month", "") and not is_initial:
        portfolio_value += monthly_dca
        dca_applied = True

    # ── 6. DECISION: generate all required orders ─────────────────────────────
    orders = []
    for symbol in asset_symbols:
        target_value  = portfolio_value * allocations[symbol]
        current_value = current_holdings[symbol]["value"]
        price         = current_holdings[symbol]["price"]

        if price is None or price <= 0:
            continue

        diff = target_value - current_value

        # Fee-aware minimum per asset class.
        # Crypto buys are exempt — the pooling phase handles their minimum.
        # Crypto sells and all stock trades are filtered individually.
        fee_key = "crypto" if is_crypto(symbol) else "stock"
        fee_rate = ROUND_TRIP_FEE[fee_key]
        est_fee  = round(abs(diff) * fee_rate, 4)
        is_crypto_buy = is_crypto(symbol) and diff > 0
        if not is_crypto_buy:
            if abs(diff) < max(1.0, portfolio_value * fee_rate * FEE_COVERAGE):
                continue

        if diff > 0:
            drift_pct = round(abs(current_value / portfolio_value - allocations[symbol]) * 100, 2)
            orders.append({
                "symbol":    symbol,
                "side":      "BUY",
                "notional":  round(diff, 2),
                "drift_pct": drift_pct,
                "est_fee":   est_fee,
                "reason":    f"Under ${diff:.2f} (target ${target_value:.2f}, have ${current_value:.2f}), est fee ~${est_fee:.3f}",
            })
        else:
            qty_to_sell = abs(diff) / price
            if current_holdings[symbol]["qty"] > 0 and qty_to_sell > 0.0001:
                qty_to_sell = min(qty_to_sell, current_holdings[symbol]["qty"])
                orders.append({
                    "symbol":   symbol,
                    "side":     "SELL",
                    "qty":      round(qty_to_sell, 6),
                    "notional": round(abs(diff), 2),
                    "est_fee":  est_fee,
                    "reason":   f"Over ${abs(diff):.2f} (target ${target_value:.2f}, have ${current_value:.2f}), est fee ~${est_fee:.3f}",
                })

    # ── 7. RECORD DECISION (before any execution) ─────────────────────────────
    # State is written now regardless of how execution goes.
    # This prevents re-triggering on the same event, double DCA, or tight loops
    # when all orders are filtered. Execution errors do not roll this back.
    if not dry_run:
        state["last_rebalance_date"] = now.isoformat()
        if dca_applied:
            state["last_dca_month"] = month_key
        state["last_regime"] = macro["regime"]
        state.setdefault("skip_count", 0)
        state.setdefault("last_skip_notification", None)
        save_state(state)

    # ── 8. EXECUTION ──────────────────────────────────────────────────────────
    executed        = []
    sells           = [o for o in orders if o["side"] == "SELL"]
    non_crypto_buys = [o for o in orders if o["side"] == "BUY" and not is_crypto(o["symbol"])]
    crypto_buys     = [o for o in orders if o["side"] == "BUY" and is_crypto(o["symbol"])]
    crypto_buys.sort(key=lambda o: o.get("drift_pct", 0), reverse=True)

    # Phase 1: sells
    for order in sells:
        if dry_run:
            executed.append({**order, "status": "DRY_RUN"})
            continue
        try:
            result = broker.sell(alpaca_symbol(order["symbol"]), qty=order["qty"])
            executed.append({**order, "status": result["status"], "order_id": result["id"]})
        except Exception as e:
            executed.append({**order, "status": f"ERROR: {e}"})

    # Re-fetch cash from broker after sells — broker is the source of truth,
    # not an in-memory counter. Sell proceeds may not settle instantly but
    # Alpaca's buying power reflects pending fills immediately on paper trading.
    if not dry_run:
        cash_available = float(broker.get_account()["cash"])
    else:
        cash_available = float(acct["cash"])

    # Phase 2: non-crypto buys
    for order in non_crypto_buys:
        if dry_run:
            executed.append({**order, "status": "DRY_RUN"})
            continue
        try:
            notional = min(order["notional"], cash_available)
            if notional < 1.0:
                executed.append({**order, "status": "SKIPPED (insufficient cash)"})
                continue
            result = broker.buy(alpaca_symbol(order["symbol"]), notional=notional)
            cash_available -= notional
            executed.append({**order, "status": result["status"], "order_id": result["id"]})
        except Exception as e:
            executed.append({**order, "status": f"ERROR: {e}"})

    # Phase 3: crypto buys with pooling
    def _place_pooled_crypto(pool_orders):
        nonlocal cash_available
        if not pool_orders:
            return
        top     = pool_orders[0]
        raw     = round(sum(o["notional"] for o in pool_orders), 2)
        notional = round(min(max(raw, MIN_CRYPTO_NOTIONAL), cash_available), 2)
        if dry_run:
            for o in pool_orders:
                executed.append({**o, "status": f"DRY_RUN (pooled ${notional:.2f} → {top['symbol']})"})
            return
        try:
            if notional < MIN_CRYPTO_NOTIONAL:
                for o in pool_orders:
                    executed.append({**o, "status": f"DEFERRED (cash ${cash_available:.2f} < min ${MIN_CRYPTO_NOTIONAL})"})
                return
            result = broker.buy(alpaca_symbol(top["symbol"]), notional=notional)
            cash_available -= notional
            for o in pool_orders:
                executed.append({**o, "status": f"POOLED→{top['symbol']} ({result['status']})",
                                 "order_id": result["id"]})
        except Exception as e:
            for o in pool_orders:
                executed.append({**o, "status": f"ERROR (pooled): {e}"})

    total_crypto_notional = round(sum(o["notional"] for o in crypto_buys), 2)
    if crypto_buys and total_crypto_notional < POOL_THRESHOLD:
        _place_pooled_crypto(crypto_buys)
    else:
        large_crypto = [o for o in crypto_buys if o["notional"] >= MIN_CRYPTO_NOTIONAL]
        small_crypto = [o for o in crypto_buys if o["notional"] <  MIN_CRYPTO_NOTIONAL]
        for order in large_crypto:
            if dry_run:
                executed.append({**order, "status": "DRY_RUN"})
                continue
            try:
                notional = min(order["notional"], cash_available)
                if notional < MIN_CRYPTO_NOTIONAL:
                    executed.append({**order, "status": f"DEFERRED (cash ${cash_available:.2f} < min ${MIN_CRYPTO_NOTIONAL})"})
                    continue
                result = broker.buy(alpaca_symbol(order["symbol"]), notional=notional)
                cash_available -= notional
                executed.append({**order, "status": result["status"], "order_id": result["id"]})
            except Exception as e:
                executed.append({**order, "status": f"ERROR: {e}"})
        if small_crypto:
            _place_pooled_crypto(small_crypto)

    # Collect execution errors for caller to alert on
    errors = [o for o in executed if str(o.get("status", "")).startswith("ERROR")]

    return {
        "action":          "INITIAL_BUY" if is_initial else "REBALANCE",
        "macro":           macro,
        "portfolio_value": round(portfolio_value, 2),
        "dca_applied":     dca_applied,
        "orders":          executed,
        "errors":          errors,
        "stale_cancelled": stale_cancelled,
        "dry_run":         dry_run,
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
