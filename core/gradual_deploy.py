"""Gradual Deployment Bear Cash Strategy.

Normal times: adaptive rebalancing (monthly bear / semi-annual bull).
Intense crash (SPY >10% below 200 SMA): bear cash mode — trim winners to war chest.
Recovery deployment:
  - Price crosses above 50-day SMA: deploy 60% of war chest (early signal)
  - Price crosses above 100-day SMA: deploy remaining war chest (confirmation)
"""

import pandas as pd
import numpy as np
from core.data import fetch_data
from core.rebalance import RebalanceEngine


class GradualDeployEngine(RebalanceEngine):

    def __init__(self, allocations: dict, initial_capital: float = 1000.0,
                 monthly_dca: float = 100.0,
                 crash_threshold_pct: float = 10.0,
                 deploy_50sma_pct: float = 0.60,
                 deploy_100sma_pct: float = 0.40,
                 bull_days: int = 126, bear_days: int = 21):
        """
        crash_threshold_pct: how far below 200 SMA to trigger bear cash mode (default 10%)
        deploy_50sma_pct: fraction of war chest to deploy when price crosses 50 SMA
        deploy_100sma_pct: fraction of war chest to deploy when price crosses 100 SMA
        """
        super().__init__(allocations, initial_capital, monthly_dca)
        self.crash_threshold = crash_threshold_pct / 100
        self.deploy_50 = deploy_50sma_pct
        self.deploy_100 = deploy_100sma_pct
        self.bull_days = bull_days
        self.bear_days = bear_days

    def run_gradual(self, prices_df: pd.DataFrame,
                    macro_prices: pd.Series) -> dict:
        sma_200 = macro_prices.rolling(window=200).mean()
        sma_100 = macro_prices.rolling(window=100).mean()
        sma_50 = macro_prices.rolling(window=50).mean()

        cash = self.initial_capital * self.allocations.get("CASH", 0)
        holdings = {}
        for symbol in self.asset_symbols:
            target_dollars = self.initial_capital * self.allocations[symbol]
            holdings[symbol] = target_dollars / prices_df[symbol].iloc[0]

        war_chest = 0.0
        equity_curve = []
        trades = []
        last_dca_month = None
        bars_since_rebalance = 0
        in_crash_mode = False
        deployed_50 = False  # track if we already deployed on 50 SMA cross
        deployed_100 = False
        bear_start_prices = {}

        for i, (date, row) in enumerate(prices_df.iterrows()):
            bars_since_rebalance += 1

            portfolio_value = cash
            for symbol in self.asset_symbols:
                portfolio_value += holdings[symbol] * row[symbol]
            total_value = portfolio_value + war_chest

            # Get macro indicators
            price = macro_prices.get(date, None)
            s200 = sma_200.get(date, None)
            s100 = sma_100.get(date, None)
            s50 = sma_50.get(date, None)

            if price is None or s200 is None or pd.isna(s200):
                is_bull = True
                is_crash = False
                dip_pct = 0
            else:
                is_bull = price > s200
                dip_pct = (s200 - price) / s200 if s200 > 0 else 0
                is_crash = dip_pct > self.crash_threshold

            # Monthly DCA
            current_month = date.month if hasattr(date, 'month') else pd.Timestamp(date).month
            current_year = date.year if hasattr(date, 'year') else pd.Timestamp(date).year
            month_key = (current_year, current_month)
            if last_dca_month is None:
                last_dca_month = month_key
            elif month_key != last_dca_month:
                if in_crash_mode:
                    war_chest += self.monthly_dca
                    trades.append({
                        "date": date, "action": "DCA→WARCHEST",
                        "detail": f"+${self.monthly_dca:.0f} (chest: ${war_chest:.0f})",
                        "portfolio_value": round(total_value + self.monthly_dca, 2),
                    })
                else:
                    cash += self.monthly_dca
                    trades.append({
                        "date": date, "action": "DCA",
                        "detail": f"+${self.monthly_dca:.0f}",
                        "portfolio_value": round(total_value + self.monthly_dca, 2),
                    })
                total_value += self.monthly_dca
                last_dca_month = month_key

            # ---- CRASH MODE TRANSITIONS ----

            # Enter crash mode
            if is_crash and not in_crash_mode:
                in_crash_mode = True
                deployed_50 = False
                deployed_100 = False
                bear_start_prices = {s: row[s] for s in self.asset_symbols}
                trades.append({
                    "date": date, "action": "CRASH DETECTED",
                    "detail": f"SPY {dip_pct*100:.1f}% below 200 SMA — "
                              f"switching to war chest mode",
                    "portfolio_value": round(total_value, 2),
                })

            # Gradual deployment during recovery
            if in_crash_mode and war_chest > 0:
                # Deploy 60% when price crosses above 50 SMA
                if (not deployed_50 and price is not None and s50 is not None
                        and not pd.isna(s50) and price > s50):
                    deploy_amount = war_chest * self.deploy_50
                    if deploy_amount > 1:
                        self._deploy_cash(holdings, row, deploy_amount,
                                          bear_start_prices, trades, date, total_value,
                                          "DEPLOY 50-SMA (60%)")
                        war_chest -= deploy_amount
                        deployed_50 = True

                # Deploy remaining when price crosses above 100 SMA
                if (not deployed_100 and price is not None and s100 is not None
                        and not pd.isna(s100) and price > s100):
                    deploy_amount = war_chest  # everything left
                    if deploy_amount > 1:
                        self._deploy_cash(holdings, row, deploy_amount,
                                          bear_start_prices, trades, date, total_value,
                                          "DEPLOY 100-SMA (remaining)")
                        war_chest -= deploy_amount
                        deployed_100 = True

            # Exit crash mode when fully deployed or back above 200 SMA
            if in_crash_mode and (war_chest < 1 and deployed_100) or (is_bull and war_chest < 1):
                if in_crash_mode:
                    in_crash_mode = False
                    trades.append({
                        "date": date, "action": "CRASH MODE EXIT",
                        "detail": "Back to normal adaptive rebalancing",
                        "portfolio_value": round(total_value, 2),
                    })

            # ---- REBALANCING ----
            rebalance_interval = self.bear_days if not is_bull else self.bull_days

            if bars_since_rebalance >= rebalance_interval and i > 0:
                bars_since_rebalance = 0

                portfolio_value = cash
                for symbol in self.asset_symbols:
                    portfolio_value += holdings[symbol] * row[symbol]

                if in_crash_mode:
                    # Only trim overweight → war chest
                    for symbol in self.asset_symbols:
                        target_value = portfolio_value * self.allocations[symbol]
                        current_value = holdings[symbol] * row[symbol]
                        excess = current_value - target_value
                        if excess > 0.01:
                            holdings[symbol] -= excess / row[symbol]
                            war_chest += excess
                            trades.append({
                                "date": date, "action": "CRASH TRIM",
                                "detail": f"Sell {symbol} ${excess:.2f} → chest "
                                          f"(${war_chest:.0f})",
                                "portfolio_value": round(total_value, 2),
                            })
                else:
                    # Normal rebalance
                    for symbol in self.asset_symbols:
                        target_value = portfolio_value * self.allocations[symbol]
                        current_value = holdings[symbol] * row[symbol]
                        diff = target_value - current_value
                        if abs(diff) > 0.01:
                            holdings[symbol] += diff / row[symbol]
                            cash -= diff
                    target_cash = portfolio_value * self.allocations.get("CASH", 0)
                    cash = target_cash

            # Record equity
            total_eq = cash + war_chest
            for symbol in self.asset_symbols:
                total_eq += holdings[symbol] * row[symbol]
            equity_curve.append({"date": date, "equity": round(total_eq, 2)})

        # Final
        final_row = prices_df.iloc[-1]
        final_value = cash + war_chest
        for symbol in self.asset_symbols:
            final_value += holdings[symbol] * final_row[symbol]

        months = len(set((d.year if hasattr(d, 'year') else pd.Timestamp(d).year,
                          d.month if hasattr(d, 'month') else pd.Timestamp(d).month)
                         for d in prices_df.index)) - 1
        total_invested = self.initial_capital + (months * self.monthly_dca)

        # Buy-and-hold
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
                    bh_holdings[symbol] += (self.monthly_dca * self.allocations[symbol]) / row[symbol]
                cash_bh += self.monthly_dca * self.allocations.get("CASH", 0)
        bh_final = cash_bh
        for symbol in self.asset_symbols:
            bh_final += bh_holdings[symbol] * final_row[symbol]

        equity_df = pd.DataFrame(equity_curve)

        return {
            "mode": "Gradual Deploy",
            "initial_capital": self.initial_capital,
            "total_invested": round(total_invested, 2),
            "final_value": round(final_value, 2),
            "total_return_pct": round((final_value - total_invested) / total_invested * 100, 2),
            "war_chest_remaining": round(war_chest, 2),
            "rebalance_count": sum(1 for t in trades if "TRIM" in t["action"]),
            "buyhold_final": round(bh_final, 2),
            "buyhold_return_pct": round((bh_final - total_invested) / total_invested * 100, 2),
            "beat_buyhold_pct": round((final_value - bh_final) / bh_final * 100, 2),
            "max_drawdown_pct": round(self._calc_max_drawdown(equity_df), 2),
            "equity_curve": equity_df,
            "trades": trades,
        }

    def _deploy_cash(self, holdings, row, amount, bear_start_prices,
                     trades, date, total_value, label):
        """Deploy cash weighted toward assets that dropped most."""
        drops = {}
        for symbol in self.asset_symbols:
            start_p = bear_start_prices.get(symbol, row[symbol])
            current_p = row[symbol]
            drop_pct = (start_p - current_p) / start_p if start_p > 0 else 0
            drops[symbol] = max(drop_pct, 0)

        total_drop = sum(drops.values())
        if total_drop > 0:
            for symbol in self.asset_symbols:
                weight = drops[symbol] / total_drop
                dollars = amount * weight
                if dollars > 0.01:
                    holdings[symbol] += dollars / row[symbol]
        else:
            for symbol in self.asset_symbols:
                dollars = amount * self.allocations[symbol]
                holdings[symbol] += dollars / row[symbol]

        trades.append({
            "date": date, "action": label,
            "detail": f"${amount:.2f} deployed (recovery-weighted)",
            "portfolio_value": round(total_value, 2),
        })


def run_full_comparison(allocations, initial_capital=1000, monthly_dca=100,
                        cache_dir="data/", start=None, end=None, period="2y"):
    """Run all strategies head-to-head."""
    from core.adaptive_rebalance import AdaptiveRebalanceEngine
    from core.bear_cash_rebalance import BearCashEngine

    spy_df = fetch_data("SPY", period=period, interval="1d",
                        cache_dir=cache_dir, cache_max_hours=24,
                        start=start, end=end)
    spy_close = spy_df["Close"].copy()
    spy_close.index = pd.to_datetime(spy_close.index, utc=True).normalize().tz_localize(None)
    spy_close = spy_close[~spy_close.index.duplicated(keep="last")]

    results = []

    # Fixed monthly
    fixed = RebalanceEngine(allocations, initial_capital, monthly_dca)
    prices = fixed.fetch_all_data(period=period, interval="1d",
                                  cache_dir=cache_dir, start=start, end=end)
    macro = spy_close.reindex(prices.index, method="ffill")

    r = fixed.run(prices, rebalance_every_n=21)
    r["mode"] = "Fixed Monthly"
    results.append(r)

    # Adaptive
    adaptive = AdaptiveRebalanceEngine(allocations, initial_capital, monthly_dca,
                                       bull_days=126, bear_days=21)
    r = adaptive.run_adaptive(prices, macro)
    results.append(r)

    # Bear Cash (proportional)
    bc = BearCashEngine(allocations, initial_capital, monthly_dca,
                        deploy_mode="proportional", bull_days=126, bear_days=21)
    r = bc.run_bear_cash(prices, macro)
    results.append(r)

    # Gradual Deploy (the new one)
    gd = GradualDeployEngine(allocations, initial_capital, monthly_dca,
                             crash_threshold_pct=10, deploy_50sma_pct=0.60,
                             deploy_100sma_pct=0.40, bull_days=126, bear_days=21)
    r = gd.run_gradual(prices, macro)
    results.append(r)

    return results
