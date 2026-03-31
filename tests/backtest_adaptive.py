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
BEAR_DAYS         = 21
DRIFT_THRESHOLD   = 5.0   # % drift from target to trigger early rebalance


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


def _run_adaptive(prices: pd.DataFrame) -> dict:
    """Adaptive bull/bear rebalancer — mirrors live bot logic."""
    sma200 = prices["SPY"].rolling(200).mean()

    holdings        = {s: INITIAL_CAPITAL * ALLOCATIONS[s] / prices[s].iloc[0] for s in prices.columns}
    cash            = 0.0
    last_m          = None
    n_months        = 0
    last_rebal_idx  = 0
    rebal_log       = []
    eq              = []

    for i, (date, row) in enumerate(prices.iterrows()):
        pv = cash + sum(holdings[s] * row[s] for s in prices.columns)

        # DCA
        mk = (date.year, date.month)
        if last_m is None:
            last_m = mk
        elif mk != last_m:
            last_m = mk
            n_months += 1
            cash += MONTHLY_DCA
            pv   += MONTHLY_DCA

        # Regime
        spy_sma = sma200.iloc[i]
        if pd.isna(spy_sma):
            regime   = "BULL"
            interval = BULL_DAYS
        else:
            regime   = "BULL" if row["SPY"] > spy_sma else "BEAR"
            interval = BEAR_DAYS if regime == "BEAR" else BULL_DAYS

        days_since   = i - last_rebal_idx
        time_trigger = days_since >= interval

        # Drift trigger
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
                target  = pv * ALLOCATIONS[s]
                current = holdings[s] * row[s]
                diff    = target - current
                holdings[s] += diff / row[s]
                cash -= diff
            cash = 0.0
            last_rebal_idx = i
            rebal_log.append({"date": date, "regime": regime, "trigger": trigger,
                               "pv": round(pv, 2)})

        eq.append(cash + sum(holdings[s] * row[s] for s in prices.columns))

    total_invested = INITIAL_CAPITAL + n_months * MONTHLY_DCA
    final_value    = eq[-1]
    eq_s = pd.Series(eq)
    peak = eq_s.expanding().max()
    mdd  = ((peak - eq_s) / peak * 100).max()

    bull_rebal = sum(1 for r in rebal_log if r["regime"] == "BULL")
    bear_rebal = sum(1 for r in rebal_log if r["regime"] == "BEAR")
    drift_rebal = sum(1 for r in rebal_log if r["trigger"] == "DRIFT")

    return {
        "label":            "Adaptive (BOT)",
        "total_invested":   round(total_invested, 2),
        "final_value":      round(final_value, 2),
        "total_return_pct": round((final_value - total_invested) / total_invested * 100, 2),
        "rebalance_count":  len(rebal_log),
        "bull_rebal":       bull_rebal,
        "bear_rebal":       bear_rebal,
        "drift_rebal":      drift_rebal,
        "max_drawdown_pct": round(mdd, 2),
        "rebalance_log":    rebal_log,
    }


# ── Report ─────────────────────────────────────────────────────────────────────

def run_period(label: str, start: str, end: str):
    print(f"\n{'='*72}")
    print(f"  Period: {label}  ({start} → {end})")
    print(f"  Initial: ${INITIAL_CAPITAL:,.0f}  |  DCA: ${MONTHLY_DCA:.0f}/mo")
    print(f"{'='*72}")

    try:
        prices = fetch_prices(start, end)
    except Exception as e:
        print(f"  ❌ Data fetch failed: {e}")
        return

    if len(prices) < 250:
        print(f"  ⚠️  Only {len(prices)} bars — too short for meaningful test.")
        return

    bh      = _buy_and_hold(prices)
    monthly = _run_fixed(prices, 21,  "Fixed monthly  (21d)")
    semi    = _run_fixed(prices, 126, "Fixed semi-ann (126d)")
    bot     = _run_adaptive(prices)

    bh_final = bh["final_value"]

    print(f"\n  {'Strategy':<26} {'Invested':>10} {'Final':>10} {'Return':>9} "
          f"{'vs B&H':>9} {'Rebal#':>7} {'MaxDD':>8}")
    print(f"  {'-'*76}")

    for r in [bh, monthly, semi, bot]:
        vs_bh = f"{(r['final_value'] - bh_final) / bh_final * 100:+.2f}%" if r["label"] != "Buy & Hold" else "  base"
        extra = ""
        if r["label"] == "Adaptive (BOT)":
            extra = (f"  ↳ BULL:{r.get('bull_rebal',0)} BEAR:{r.get('bear_rebal',0)} "
                     f"DRIFT:{r.get('drift_rebal',0)}")
        print(f"  {r['label']:<26} "
              f"${r['total_invested']:>9,.2f} "
              f"${r['final_value']:>9,.2f} "
              f"{r['total_return_pct']:>+8.2f}% "
              f"{vs_bh:>9} "
              f"{r['rebalance_count']:>7} "
              f"{r['max_drawdown_pct']:>7.2f}%"
              f"{extra}")

    # Winner
    strategies = [bh, monthly, semi, bot]
    best_ret = max(strategies, key=lambda r: r["total_return_pct"])
    best_dd  = min(strategies, key=lambda r: r["max_drawdown_pct"])
    print(f"\n  Best return:   {best_ret['label']} ({best_ret['total_return_pct']:+.2f}%)")
    print(f"  Best drawdown: {best_dd['label']} ({best_dd['max_drawdown_pct']:.2f}%)")


def main():
    today = datetime.today().strftime("%Y-%m-%d")
    print(f"\n{'#'*72}")
    print(f"  ADAPTIVE BOT BACKTEST REPORT — {today}")
    print(f"  Portfolio: SPY 30% | QQQ 10% | VGK 10% | GLD 25% | BTC 15% | ETH 10%")
    print(f"  Bot logic: BULL={BULL_DAYS}d | BEAR={BEAR_DAYS}d | Drift>{DRIFT_THRESHOLD}%")
    print(f"{'#'*72}")

    periods = [
        ("1 year  (2025–2026)", "2025-04-01", today),
        ("2 years (2024–2026)", "2024-04-01", today),
        ("3 years (2023–2026)", "2023-04-01", today),
        ("5 years (2021–2026)", "2021-04-01", today),
    ]

    for label, start, end in periods:
        run_period(label, start, end)

    print(f"\n{'#'*72}\n")


if __name__ == "__main__":
    main()
