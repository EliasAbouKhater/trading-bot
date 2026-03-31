"""Adaptive bull/bear rebalancer backtest.

Simulates the live bot's exact decision logic on historical data:
  - Regime detection: SPY vs 200-day SMA → BULL / BEAR
  - BULL mode: rebalance every 126 trading days (~6 months)
  - BEAR mode: rebalance every 21 trading days (~1 month)
  - Drift trigger: any asset drifts > DRIFT_THRESHOLD_PCT from target
  - Monthly DCA

Compares against:
  - Buy & Hold (same allocations + DCA, never rebalance)
  - Fixed monthly rebalance (21d)
  - Fixed semi-annual rebalance (126d)

Periods tested: 1y, 2y, 3y, 5y
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

# ── Config (mirrors config.yaml) ──────────────────────────────────────────────

ALLOCATIONS = {
    "SPY":     0.30,
    "QQQ":     0.10,
    "VGK":     0.10,
    "GLD":     0.25,
    "BTC-USD": 0.15,
    "ETH-USD": 0.10,
}
# yfinance ticker → display name
DISPLAY = {
    "SPY": "SPY", "QQQ": "QQQ", "VGK": "VGK",
    "GLD": "GLD", "BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD",
}

INITIAL_CAPITAL   = 1000.0
MONTHLY_DCA       = 100.0
BULL_DAYS         = 126
DRIFT_THRESHOLD   = 5.0   # % drift from target to trigger early rebalance

BEAR_CANDIDATES   = [21, 30, 35, 42]   # sweep these, pick best
MA_TYPES          = ["SMA", "EMA"]      # regime detection method


# ── Data fetch ─────────────────────────────────────────────────────────────────

def fetch_prices(start: str, end: str) -> pd.DataFrame:
    tickers = list(ALLOCATIONS.keys())
    raw = yf.download(tickers, start=start, end=end, auto_adjust=True,
                      progress=False, group_by="ticker")
    # Extract Close for each ticker
    frames = {}
    for t in tickers:
        try:
            frames[t] = raw[t]["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw["Close"]
        except Exception:
            frames[t] = raw[(t, "Close")]

    df = pd.DataFrame(frames).dropna()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    return df


# ── Backtest engines ───────────────────────────────────────────────────────────

def _buy_and_hold(prices: pd.DataFrame) -> dict:
    """Pure buy-and-hold. DCA added proportionally each month."""
    holdings  = {s: INITIAL_CAPITAL * ALLOCATIONS[s] / prices[s].iloc[0] for s in prices.columns}
    cash      = 0.0
    last_m    = None
    n_months  = 0

    for date, row in prices.iterrows():
        mk = (date.year, date.month)
        if last_m is None:
            last_m = mk
        elif mk != last_m:
            last_m = mk
            n_months += 1
            for s in prices.columns:
                holdings[s] += MONTHLY_DCA * ALLOCATIONS[s] / row[s]

    final_row    = prices.iloc[-1]
    final_value  = cash + sum(holdings[s] * final_row[s] for s in prices.columns)
    total_invested = INITIAL_CAPITAL + n_months * MONTHLY_DCA

    eq = []
    holdings2 = {s: INITIAL_CAPITAL * ALLOCATIONS[s] / prices[s].iloc[0] for s in prices.columns}
    last_m2 = None
    for date, row in prices.iterrows():
        mk = (date.year, date.month)
        if last_m2 is None:
            last_m2 = mk
        elif mk != last_m2:
            last_m2 = mk
            for s in prices.columns:
                holdings2[s] += MONTHLY_DCA * ALLOCATIONS[s] / row[s]
        pv = sum(holdings2[s] * row[s] for s in prices.columns)
        eq.append(pv)

    eq_s = pd.Series(eq)
    peak = eq_s.expanding().max()
    mdd  = ((peak - eq_s) / peak * 100).max()

    return {
        "label":            "Buy & Hold",
        "total_invested":   round(total_invested, 2),
        "final_value":      round(final_value, 2),
        "total_return_pct": round((final_value - total_invested) / total_invested * 100, 2),
        "rebalance_count":  0,
        "max_drawdown_pct": round(mdd, 2),
    }


def _run_fixed(prices: pd.DataFrame, every_n: int, label: str) -> dict:
    """Fixed-interval rebalancer. Baseline comparison."""
    holdings  = {s: INITIAL_CAPITAL * ALLOCATIONS[s] / prices[s].iloc[0] for s in prices.columns}
    cash      = 0.0
    last_m    = None
    n_months  = 0
    bars_since = 0
    rebal_count = 0
    eq = []

    for i, (date, row) in enumerate(prices.iterrows()):
        pv = cash + sum(holdings[s] * row[s] for s in prices.columns)

        mk = (date.year, date.month)
        if last_m is None:
            last_m = mk
        elif mk != last_m:
            last_m = mk
            n_months += 1
            cash += MONTHLY_DCA
            pv   += MONTHLY_DCA

        bars_since += 1
        if bars_since >= every_n and i > 0:
            bars_since = 0
            rebal_count += 1
            for s in prices.columns:
                target = pv * ALLOCATIONS[s]
                current = holdings[s] * row[s]
                diff = target - current
                holdings[s] += diff / row[s]
                cash -= diff
            cash = 0.0  # all cash deployed

        eq.append(cash + sum(holdings[s] * row[s] for s in prices.columns))

    total_invested = INITIAL_CAPITAL + n_months * MONTHLY_DCA
    final_value    = eq[-1]
    eq_s = pd.Series(eq)
    peak = eq_s.expanding().max()
    mdd  = ((peak - eq_s) / peak * 100).max()

    return {
        "label":            label,
        "total_invested":   round(total_invested, 2),
        "final_value":      round(final_value, 2),
        "total_return_pct": round((final_value - total_invested) / total_invested * 100, 2),
        "rebalance_count":  rebal_count,
        "max_drawdown_pct": round(mdd, 2),
    }


def _run_adaptive(prices: pd.DataFrame, bear_days: int, ma_type: str = "SMA") -> dict:
    """Adaptive bull/bear rebalancer.
    ma_type: 'SMA' or 'EMA' for regime detection (200-period).
    """
    if ma_type == "EMA":
        ma200 = prices["SPY"].ewm(span=200, adjust=False).mean()
        label = f"Adaptive EMA bear={bear_days}d"
    else:
        ma200 = prices["SPY"].rolling(200).mean()
        label = f"Adaptive SMA bear={bear_days}d"

    holdings       = {s: INITIAL_CAPITAL * ALLOCATIONS[s] / prices[s].iloc[0] for s in prices.columns}
    cash           = 0.0
    last_m         = None
    n_months       = 0
    last_rebal_idx = 0
    rebal_log      = []
    eq             = []

    for i, (date, row) in enumerate(prices.iterrows()):
        pv = cash + sum(holdings[s] * row[s] for s in prices.columns)

        mk = (date.year, date.month)
        if last_m is None:
            last_m = mk
        elif mk != last_m:
            last_m = mk
            n_months += 1
            cash += MONTHLY_DCA
            pv   += MONTHLY_DCA

        ma_val = ma200.iloc[i]
        if pd.isna(ma_val):
            regime   = "BULL"
            interval = BULL_DAYS
        else:
            regime   = "BULL" if row["SPY"] > ma_val else "BEAR"
            interval = bear_days if regime == "BEAR" else BULL_DAYS

        days_since   = i - last_rebal_idx
        time_trigger = days_since >= interval

        drift_trigger = False
        for s in prices.columns:
            target  = ALLOCATIONS[s]
            current = (holdings[s] * row[s]) / pv if pv > 0 else 0
            if abs(current - target) * 100 > DRIFT_THRESHOLD:
                drift_trigger = True
                break

        if (time_trigger or drift_trigger) and i > 0:
            trigger = "TIME" if time_trigger else "DRIFT"
            for s in prices.columns:
                diff = pv * ALLOCATIONS[s] - holdings[s] * row[s]
                holdings[s] += diff / row[s]
                cash -= diff
            cash = 0.0
            last_rebal_idx = i
            rebal_log.append({"date": date, "regime": regime, "trigger": trigger})

        eq.append(cash + sum(holdings[s] * row[s] for s in prices.columns))

    total_invested = INITIAL_CAPITAL + n_months * MONTHLY_DCA
    final_value    = eq[-1]
    eq_s = pd.Series(eq)
    mdd  = ((eq_s.expanding().max() - eq_s) / eq_s.expanding().max() * 100).max()

    return {
        "label":            label,
        "bear_days":        bear_days,
        "ma_type":          ma_type,
        "total_invested":   round(total_invested, 2),
        "final_value":      round(final_value, 2),
        "total_return_pct": round((final_value - total_invested) / total_invested * 100, 2),
        "rebalance_count":  len(rebal_log),
        "bull_rebal":       sum(1 for r in rebal_log if r["regime"] == "BULL"),
        "bear_rebal":       sum(1 for r in rebal_log if r["regime"] == "BEAR"),
        "drift_rebal":      sum(1 for r in rebal_log if r["trigger"] == "DRIFT"),
        "max_drawdown_pct": round(mdd, 2),
    }


# ── Report ─────────────────────────────────────────────────────────────────────

def run_period(label: str, start: str, end: str) -> list:
    """Run all strategy variants for one period. Returns list of result dicts."""
    print(f"\n{'='*80}")
    print(f"  Period: {label}  ({start} → {end})")
    print(f"  Initial: ${INITIAL_CAPITAL:,.0f}  |  DCA: ${MONTHLY_DCA:.0f}/mo")
    print(f"{'='*80}")

    try:
        prices = fetch_prices(start, end)
    except Exception as e:
        print(f"  ❌ Data fetch failed: {e}")
        return []

    if len(prices) < 250:
        print(f"  ⚠️  Only {len(prices)} bars — too short.")
        return []

    bh      = _buy_and_hold(prices)
    monthly = _run_fixed(prices, 21,  "Fixed monthly (21d)")
    semi    = _run_fixed(prices, 126, "Fixed semi-ann (126d)")

    variants = []
    for bear_d in BEAR_CANDIDATES:
        for ma in MA_TYPES:
            variants.append(_run_adaptive(prices, bear_d, ma))

    bh_final  = bh["final_value"]
    all_strats = [bh, monthly, semi] + variants

    print(f"\n  {'Strategy':<32} {'Return':>9} {'vs B&H':>9} "
          f"{'Rebal#':>7} {'MaxDD':>8}  {'Breakdown'}")
    print(f"  {'-'*82}")

    print(f"  {'Buy & Hold':<32} {bh['total_return_pct']:>+8.2f}%   {'base':>8}  "
          f"{0:>7}  {bh['max_drawdown_pct']:>7.2f}%")
    print(f"  {'Fixed monthly (21d)':<32} {monthly['total_return_pct']:>+8.2f}%  "
          f"{(monthly['final_value']-bh_final)/bh_final*100:>+8.2f}%  "
          f"{monthly['rebalance_count']:>7}  {monthly['max_drawdown_pct']:>7.2f}%")
    print(f"  {'Fixed semi-ann (126d)':<32} {semi['total_return_pct']:>+8.2f}%  "
          f"{(semi['final_value']-bh_final)/bh_final*100:>+8.2f}%  "
          f"{semi['rebalance_count']:>7}  {semi['max_drawdown_pct']:>7.2f}%")
    print(f"  {'-'*82}")

    for r in variants:
        vs_bh   = (r["final_value"] - bh_final) / bh_final * 100
        detail  = f"BULL:{r['bull_rebal']} BEAR:{r['bear_rebal']} DRIFT:{r['drift_rebal']}"
        print(f"  {r['label']:<32} {r['total_return_pct']:>+8.2f}%  "
              f"{vs_bh:>+8.2f}%  "
              f"{r['rebalance_count']:>7}  {r['max_drawdown_pct']:>7.2f}%  {detail}")

    best_var = max(variants, key=lambda r: r["total_return_pct"])
    best_dd  = min(variants, key=lambda r: r["max_drawdown_pct"])
    print(f"\n  ★ Best return (bot variants): {best_var['label']} "
          f"({best_var['total_return_pct']:+.2f}%  vs B&H {(best_var['final_value']-bh_final)/bh_final*100:+.2f}%)")
    print(f"  ★ Best drawdown (bot variants): {best_dd['label']} "
          f"({best_dd['max_drawdown_pct']:.2f}%)")

    return [{"period": label, **r} for r in variants]


def main():
    today = datetime.today().strftime("%Y-%m-%d")
    print(f"\n{'#'*80}")
    print(f"  ADAPTIVE BOT BACKTEST — {today}")
    print(f"  Portfolio: SPY 30% | QQQ 10% | VGK 10% | GLD 25% | BTC 15% | ETH 10%")
    print(f"  BULL={BULL_DAYS}d | BEAR sweep={BEAR_CANDIDATES} | MA={MA_TYPES} | Drift>{DRIFT_THRESHOLD}%")
    print(f"{'#'*80}")

    periods = [
        ("1 year  (2025–2026)", "2025-04-01", today),
        ("2 years (2024–2026)", "2024-04-01", today),
        ("3 years (2023–2026)", "2023-04-01", today),
        ("5 years (2021–2026)", "2021-04-01", today),
    ]

    all_results = []
    for label, start, end in periods:
        all_results.extend(run_period(label, start, end))

    # ── Cross-period summary: rank each variant by avg return vs B&H ──────────
    if all_results:
        print(f"\n\n{'#'*80}")
        print(f"  CROSS-PERIOD SUMMARY — avg return across all periods")
        print(f"{'#'*80}")

        from collections import defaultdict
        totals = defaultdict(list)
        for r in all_results:
            totals[r["label"]].append(r["total_return_pct"])

        ranked = sorted(totals.items(), key=lambda x: -sum(x[1])/len(x[1]))
        print(f"\n  {'Variant':<32} {'Avg Return':>12}  {'1y':>8} {'2y':>8} {'3y':>8} {'5y':>8}")
        print(f"  {'-'*72}")
        for name, returns in ranked:
            avg = sum(returns) / len(returns)
            cols = "  ".join(f"{r:>+7.2f}%" for r in returns)
            print(f"  {name:<32} {avg:>+11.2f}%  {cols}")

        winner = ranked[0][0]
        print(f"\n  ★ Overall winner: {winner}")
        print(f"{'#'*80}\n")


if __name__ == "__main__":
    main()
