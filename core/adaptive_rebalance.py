"""Adaptive rebalancing — adjusts frequency based on market macro conditions.

Bull market (SPY > 200-day SMA): rebalance less often (semi-annual) — let winners run.
Bear market (SPY < 200-day SMA): rebalance more often (monthly) — protect capital.

Drift trigger: each asset has its own threshold derived from historical volatility.
If any asset drifts beyond its regime-adjusted threshold, rebalance immediately
regardless of the time schedule. Sells overweight assets to fund the underweights.
"""

import pandas as pd
import numpy as np
from core.data import fetch_data
from core.rebalance import RebalanceEngine


class AdaptiveRebalanceEngine(RebalanceEngine):
    """Extends RebalanceEngine with macro-aware frequency and per-asset drift triggers."""

    def __init__(self, allocations: dict, initial_capital: float = 1000.0,
                 monthly_dca: float = 100.0, macro_symbol: str = "SPY",
                 macro_sma: int = 200, bull_days: int = 126,
                 bear_days: int = 21,
                 drift_vol_multiplier: float = 0.20,
                 drift_min_pct: float = 2.0,
                 drift_max_pct: float = 20.0,
                 drift_bull_factor: float = 1.5,
                 drift_bear_factor: float = 0.7):
        """
        macro_symbol:        asset to use as macro indicator (SPY = broad market)
        macro_sma:           SMA period for bull/bear detection
        bull_days:           time-based rebalance interval in bull market (trading days)
        bear_days:           time-based rebalance interval in bear market (trading days)
        drift_vol_multiplier: threshold = multiplier × annualized_vol (per asset)
        drift_min_pct:       floor for drift threshold (percentage points)
        drift_max_pct:       cap for drift threshold (percentage points)
        drift_bull_factor:   widen thresholds in bull (e.g. 1.5 = 50% looser)
        drift_bear_factor:   tighten thresholds in bear (e.g. 0.7 = 30% tighter)
        """
        super().__init__(allocations, initial_capital, monthly_dca)
        self.macro_symbol = macro_symbol
        self.macro_sma = macro_sma
        self.bull_days = bull_days
        self.bear_days = bear_days
        self.drift_vol_multiplier = drift_vol_multiplier
        self.drift_min_pct = drift_min_pct
        self.drift_max_pct = drift_max_pct
        self.drift_bull_factor = drift_bull_factor
        self.drift_bear_factor = drift_bear_factor

    def compute_vol_thresholds(self, prices_df: pd.DataFrame) -> dict:
        """Compute per-asset base drift thresholds from annualized historical volatility.

        threshold_i = vol_multiplier × annualized_vol_i, clamped to [min_pct, max_pct].
        These are in percentage points (e.g. 12.0 means trigger if drift > 12pp).
        """
        thresholds = {}
        for symbol in self.asset_symbols:
            if symbol not in prices_df.columns:
                thresholds[symbol] = self.drift_min_pct
                continue
            returns = prices_df[symbol].pct_change().dropna()
            ann_vol_pct = returns.std() * np.sqrt(252) * 100  # annualized, in %
            raw = self.drift_vol_multiplier * ann_vol_pct
            thresholds[symbol] = max(self.drift_min_pct, min(self.drift_max_pct, raw))
        return thresholds

    def _check_drift(self, holdings: dict, row: pd.Series,
                     portfolio_value: float, regime_thresholds: dict) -> list:
        """Return list of (symbol, drift_pp, threshold) for assets that breached threshold."""
        violators = []
        for symbol in self.asset_symbols:
            current_weight = (holdings[symbol] * row[symbol]) / portfolio_value * 100
            target_weight = self.allocations[symbol] * 100
            drift = abs(current_weight - target_weight)
            if drift > regime_thresholds[symbol]:
                violators.append((symbol, round(drift, 2), round(regime_thresholds[symbol], 2)))
        return violators

    def _execute_rebalance(self, holdings: dict, row: pd.Series,
                           portfolio_value: float, cash: float) -> tuple[dict, float, list]:
        """Redistribute portfolio to target allocations. Returns updated holdings, cash, detail."""
        detail = []
        for symbol in self.asset_symbols:
            target_value = portfolio_value * self.allocations[symbol]
            current_value = holdings[symbol] * row[symbol]
            diff = target_value - current_value
            if abs(diff) > 0.01:
                holdings[symbol] += diff / row[symbol]
                cash -= diff  # sell overweight (diff<0 → cash increases), buy underweight (diff>0 → cash decreases)
                action = "BUY" if diff > 0 else "SELL"
                detail.append(f"{action} {symbol} ${abs(diff):.2f}")
        cash = portfolio_value * self.allocations.get("CASH", 0)
        return holdings, cash, detail

    def run_adaptive(self, prices_df: pd.DataFrame,
                     macro_prices: pd.Series) -> dict:
        """Run with adaptive time-based rebalancing + per-asset drift triggers."""
        # Pre-compute base thresholds from full price history
        base_thresholds = self.compute_vol_thresholds(prices_df)

        # Compute macro SMA
        sma = macro_prices.rolling(window=self.macro_sma).mean()

        cash = self.initial_capital * self.allocations.get("CASH", 0)
        holdings = {}
        for symbol in self.asset_symbols:
            target_dollars = self.initial_capital * self.allocations[symbol]
            holdings[symbol] = target_dollars / prices_df[symbol].iloc[0]

        equity_curve = []
        trades = []
        last_dca_month = None
        bars_since_rebalance = 0
        regime_log = []

        for i, (date, row) in enumerate(prices_df.iterrows()):
            bars_since_rebalance += 1

            portfolio_value = cash
            for symbol in self.asset_symbols:
                portfolio_value += holdings[symbol] * row[symbol]

            # Monthly DCA
            current_month = date.month if hasattr(date, 'month') else pd.Timestamp(date).month
            current_year = date.year if hasattr(date, 'year') else pd.Timestamp(date).year
            month_key = (current_year, current_month)
            if last_dca_month is None:
                last_dca_month = month_key
            elif month_key != last_dca_month:
                cash += self.monthly_dca
                portfolio_value += self.monthly_dca
                last_dca_month = month_key
                trades.append({
                    "date": date, "action": "DCA",
                    "detail": f"+${self.monthly_dca:.0f}",
                    "portfolio_value": round(portfolio_value, 2),
                })

            # Determine regime
            macro_val = macro_prices.get(date, None)
            sma_val = sma.get(date, None)
            if macro_val is not None and sma_val is not None and not pd.isna(sma_val):
                is_bull = macro_val > sma_val
            else:
                is_bull = True  # default to bull while SMA warmup period

            regime = "BULL" if is_bull else "BEAR"
            rebalance_interval = self.bull_days if is_bull else self.bear_days

            # Regime-adjusted drift thresholds
            regime_factor = self.drift_bull_factor if is_bull else self.drift_bear_factor
            regime_thresholds = {s: base_thresholds[s] * regime_factor for s in self.asset_symbols}

            if i > 0:
                # Drift trigger: check every bar, fires independently of time
                violators = self._check_drift(holdings, row, portfolio_value, regime_thresholds)
                drift_triggered = len(violators) > 0
                time_triggered = bars_since_rebalance >= rebalance_interval

                if drift_triggered or time_triggered:
                    bars_since_rebalance = 0
                    trigger = "DRIFT" if drift_triggered else f"TIME-{regime}"

                    holdings, cash, detail = self._execute_rebalance(
                        holdings, row, portfolio_value, cash
                    )

                    if detail:
                        drift_info = ""
                        if drift_triggered:
                            drift_info = " | " + ", ".join(
                                f"{s} {d:.1f}pp>{t:.1f}pp" for s, d, t in violators
                            )
                        trades.append({
                            "date": date,
                            "action": f"REBAL ({trigger})",
                            "detail": "; ".join(detail) + drift_info,
                            "portfolio_value": round(portfolio_value, 2),
                        })
                        regime_log.append({
                            "date": date,
                            "regime": regime,
                            "trigger": trigger,
                        })

            equity_curve.append({"date": date, "equity": round(portfolio_value, 2)})

        # Final value
        final_row = prices_df.iloc[-1]
        final_value = cash
        for symbol in self.asset_symbols:
            final_value += holdings[symbol] * final_row[symbol]

        # Total invested
        months = len(set((d.year if hasattr(d, 'year') else pd.Timestamp(d).year,
                          d.month if hasattr(d, 'month') else pd.Timestamp(d).month)
                         for d in prices_df.index)) - 1
        total_invested = self.initial_capital + (months * self.monthly_dca)

        # Buy-and-hold benchmark
        cash_bh = self.initial_capital * self.allocations.get("CASH", 0)
        bh_holdings = {}
        for symbol in self.asset_symbols:
            bh_holdings[symbol] = (self.initial_capital * self.allocations[symbol]) / prices_df[symbol].iloc[0]
        last_m = None
        for date, row in prices_df.iterrows():
            m_key = (date.year if hasattr(date, 'year') else pd.Timestamp(date).year,
                     date.month if hasattr(date, 'month') else pd.Timestamp(date).month)
            if last_m is None:
                last_m = m_key
            elif m_key != last_m:
                last_m = m_key
                for symbol in self.asset_symbols:
                    cash_bh += self.monthly_dca * self.allocations.get("CASH", 0)
                    bh_holdings[symbol] += (self.monthly_dca * self.allocations[symbol]) / row[symbol]
        bh_final = cash_bh
        for symbol in self.asset_symbols:
            bh_final += bh_holdings[symbol] * final_row[symbol]

        equity_df = pd.DataFrame(equity_curve)
        bull_rebalances = sum(1 for r in regime_log if r["regime"] == "BULL")
        bear_rebalances = sum(1 for r in regime_log if r["regime"] == "BEAR")
        drift_rebalances = sum(1 for r in regime_log if r["trigger"] == "DRIFT")
        time_rebalances = len(regime_log) - drift_rebalances

        return {
            "mode": f"Adaptive+Drift ({self.bear_days}d bear / {self.bull_days}d bull)",
            "initial_capital": self.initial_capital,
            "total_invested": round(total_invested, 2),
            "final_value": round(final_value, 2),
            "total_return_pct": round((final_value - total_invested) / total_invested * 100, 2),
            "profit": round(final_value - total_invested, 2),
            "rebalance_count": len(regime_log),
            "bull_rebalances": bull_rebalances,
            "bear_rebalances": bear_rebalances,
            "drift_rebalances": drift_rebalances,
            "time_rebalances": time_rebalances,
            "vol_thresholds": {s: round(base_thresholds[s], 2) for s in self.asset_symbols},
            "buyhold_final": round(bh_final, 2),
            "buyhold_return_pct": round((bh_final - total_invested) / total_invested * 100, 2),
            "beat_buyhold_pct": round((final_value - bh_final) / bh_final * 100, 2),
            "max_drawdown_pct": round(self._calc_max_drawdown(equity_df), 2),
            "equity_curve": equity_df,
            "trades": trades,
        }


def run_adaptive_vs_fixed(allocations: dict, initial_capital: float = 1000.0,
                          monthly_dca: float = 100.0, cache_dir: str = "data/",
                          start: str = None, end: str = None,
                          period: str = "2y",
                          drift_vol_multiplier: float = 0.20,
                          drift_min_pct: float = 2.0,
                          drift_max_pct: float = 20.0,
                          drift_bull_factor: float = 1.5,
                          drift_bear_factor: float = 0.7) -> list:
    """Compare adaptive+drift rebalancing vs fixed frequencies."""
    from core.rebalance import RebalanceEngine

    # Fetch macro indicator (SPY)
    spy_df = fetch_data("SPY", period=period, interval="1d",
                        cache_dir=cache_dir, cache_max_hours=24,
                        start=start, end=end)
    spy_close = spy_df["Close"].copy()
    spy_close.index = pd.to_datetime(spy_close.index, utc=True).normalize().tz_localize(None)
    spy_close = spy_close[~spy_close.index.duplicated(keep="last")]

    # Adaptive+drift engine
    adaptive = AdaptiveRebalanceEngine(
        allocations, initial_capital, monthly_dca,
        macro_symbol="SPY", macro_sma=200,
        bull_days=126, bear_days=21,
        drift_vol_multiplier=drift_vol_multiplier,
        drift_min_pct=drift_min_pct,
        drift_max_pct=drift_max_pct,
        drift_bull_factor=drift_bull_factor,
        drift_bear_factor=drift_bear_factor,
    )
    prices = adaptive.fetch_all_data(period=period, interval="1d",
                                     cache_dir=cache_dir, start=start, end=end)

    # Align macro with prices
    macro_aligned = spy_close.reindex(prices.index, method="ffill")

    results = []

    # Fixed frequencies for comparison
    fixed_engine = RebalanceEngine(allocations, initial_capital, monthly_dca)
    for n, label in [(21, "Fixed Monthly"), (126, "Fixed Semi-annual")]:
        if n <= len(prices):
            r = fixed_engine.run(prices, rebalance_every_n=n)
            r["mode"] = label
            r["bull_rebalances"] = "-"
            r["bear_rebalances"] = "-"
            r["drift_rebalances"] = "-"
            r["time_rebalances"] = "-"
            r["vol_thresholds"] = {}
            results.append(r)

    # Adaptive + drift
    r_adaptive = adaptive.run_adaptive(prices, macro_aligned)
    results.append(r_adaptive)

    return results


def test_rebalance_day(allocations: dict, initial_capital: float = 1000.0,
                       monthly_dca: float = 100.0, cache_dir: str = "data/",
                       start: str = None, end: str = None,
                       period: str = "2y") -> list:
    """Test whether the day of month for rebalancing matters.

    Simulates monthly rebalancing starting on different days (1st, 5th, 10th, 15th, 20th, 25th).
    """
    engine = RebalanceEngine(allocations, initial_capital, monthly_dca)
    prices = engine.fetch_all_data(period=period, interval="1d",
                                   cache_dir=cache_dir, start=start, end=end)

    results = []
    # Test offsets: rebalance on trading day 1, 5, 10, 15, 20 of each month
    for offset in [0, 4, 9, 14, 19]:
        # Shift the start point to simulate different "day of month"
        shifted_prices = prices.iloc[offset:]
        if len(shifted_prices) < 21:
            continue
        r = engine.run(shifted_prices, rebalance_every_n=21)

        # Label with approximate day
        day_label = ["1st", "5th", "10th", "15th", "20th"][offset // 5 if offset > 0 else 0]
        r["day_label"] = f"~{day_label} of month"
        results.append(r)

    return results
