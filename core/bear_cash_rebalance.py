"""Bear Cash Rebalancing — Buffett-style defensive strategy.

BEAR market: sell winners that exceeded target ratio, but DON'T buy losers.
             Park excess in cash (war chest).
BULL market: deploy accumulated war chest back into the portfolio.
             Two modes: proportional (to target ratios) or recovery-weighted
             (more to assets that fell the most during bear).
"""

import pandas as pd
import numpy as np
from core.data import fetch_data
from core.rebalance import RebalanceEngine


class BearCashEngine(RebalanceEngine):

    def __init__(self, allocations: dict, initial_capital: float = 1000.0,
                 monthly_dca: float = 100.0, macro_symbol: str = "SPY",
                 macro_sma: int = 200, bull_days: int = 126,
                 bear_days: int = 21, deploy_mode: str = "proportional"):
        """
        deploy_mode: how to deploy war chest when bull returns
            "proportional" — deploy to target allocation ratios
            "recovery" — deploy more to assets that fell most during bear
        """
        super().__init__(allocations, initial_capital, monthly_dca)
        self.macro_symbol = macro_symbol
        self.macro_sma = macro_sma
        self.bull_days = bull_days
        self.bear_days = bear_days
        self.deploy_mode = deploy_mode

    def run_bear_cash(self, prices_df: pd.DataFrame,
                      macro_prices: pd.Series) -> dict:
        sma = macro_prices.rolling(window=self.macro_sma).mean()

        cash = self.initial_capital * self.allocations.get("CASH", 0)
        holdings = {}
        for symbol in self.asset_symbols:
            target_dollars = self.initial_capital * self.allocations[symbol]
            holdings[symbol] = target_dollars / prices_df[symbol].iloc[0]

        war_chest = 0.0  # bear-market cash accumulation
        equity_curve = []
        trades = []
        last_dca_month = None
        bars_since_rebalance = 0
        was_bear = False
        bear_start_prices = {}  # track prices at bear entry for recovery mode

        for i, (date, row) in enumerate(prices_df.iterrows()):
            bars_since_rebalance += 1

            # Portfolio value (excluding war chest for allocation calc)
            portfolio_value = cash
            for symbol in self.asset_symbols:
                portfolio_value += holdings[symbol] * row[symbol]
            total_value = portfolio_value + war_chest

            # Monthly DCA
            current_month = date.month if hasattr(date, 'month') else pd.Timestamp(date).month
            current_year = date.year if hasattr(date, 'year') else pd.Timestamp(date).year
            month_key = (current_year, current_month)
            if last_dca_month is None:
                last_dca_month = month_key
            elif month_key != last_dca_month:
                # During bear: DCA goes to war chest too
                macro_val = macro_prices.get(date, None)
                sma_val = sma.get(date, None)
                is_bear_now = (macro_val is not None and sma_val is not None
                               and not pd.isna(sma_val) and macro_val < sma_val)
                if is_bear_now:
                    war_chest += self.monthly_dca
                    trades.append({
                        "date": date, "action": "DCA→CASH",
                        "detail": f"+${self.monthly_dca:.0f} to war chest "
                                  f"(total: ${war_chest:.0f})",
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

            # Determine regime
            macro_val = macro_prices.get(date, None)
            sma_val = sma.get(date, None)
            if macro_val is not None and sma_val is not None and not pd.isna(sma_val):
                is_bull = macro_val > sma_val
            else:
                is_bull = True

            rebalance_interval = self.bull_days if is_bull else self.bear_days

            # Detect regime transition
            is_bear = not is_bull
            if is_bear and not was_bear:
                # Entering bear — record prices for recovery weighting
                bear_start_prices = {s: row[s] for s in self.asset_symbols}
                trades.append({
                    "date": date, "action": "BEAR START",
                    "detail": "Switching to defensive mode",
                    "portfolio_value": round(total_value, 2),
                })

            if is_bull and was_bear and war_chest > 0:
                # BULL RETURNED — deploy war chest!
                deploy_amount = war_chest
                trades.append({
                    "date": date, "action": "DEPLOY WAR CHEST",
                    "detail": f"Deploying ${deploy_amount:.2f} back to portfolio",
                    "portfolio_value": round(total_value, 2),
                })

                if self.deploy_mode == "recovery" and bear_start_prices:
                    # Weight more to assets that fell the most
                    drops = {}
                    for symbol in self.asset_symbols:
                        start_p = bear_start_prices.get(symbol, row[symbol])
                        current_p = row[symbol]
                        drop_pct = (start_p - current_p) / start_p if start_p > 0 else 0
                        drops[symbol] = max(drop_pct, 0)  # only count drops

                    total_drop = sum(drops.values())
                    if total_drop > 0:
                        # Proportional to how much each dropped
                        for symbol in self.asset_symbols:
                            weight = drops[symbol] / total_drop
                            dollars = deploy_amount * weight
                            if dollars > 0:
                                units = dollars / row[symbol]
                                holdings[symbol] += units
                                trades.append({
                                    "date": date, "action": "RECOVERY BUY",
                                    "detail": f"Buy {symbol} ${dollars:.2f} "
                                              f"(dropped {drops[symbol]*100:.1f}%)",
                                    "portfolio_value": round(total_value, 2),
                                })
                    else:
                        # Nothing dropped — deploy proportionally
                        for symbol in self.asset_symbols:
                            dollars = deploy_amount * self.allocations[symbol]
                            holdings[symbol] += dollars / row[symbol]
                else:
                    # Proportional deployment to target ratios
                    for symbol in self.asset_symbols:
                        dollars = deploy_amount * self.allocations[symbol]
                        holdings[symbol] += dollars / row[symbol]

                war_chest = 0.0
                bear_start_prices = {}

            was_bear = is_bear

            # Rebalance check
            if bars_since_rebalance >= rebalance_interval and i > 0:
                bars_since_rebalance = 0
                regime = "BULL" if is_bull else "BEAR"

                # Recalculate portfolio value
                portfolio_value = cash
                for symbol in self.asset_symbols:
                    portfolio_value += holdings[symbol] * row[symbol]

                if is_bear:
                    # BEAR REBALANCE: only sell winners, park in war chest
                    for symbol in self.asset_symbols:
                        target_value = portfolio_value * self.allocations[symbol]
                        current_value = holdings[symbol] * row[symbol]
                        excess = current_value - target_value

                        if excess > 0.01:  # only sell overweight assets
                            units_to_sell = excess / row[symbol]
                            holdings[symbol] -= units_to_sell
                            war_chest += excess
                            trades.append({
                                "date": date, "action": "BEAR TRIM",
                                "detail": f"Sell {symbol} ${excess:.2f} → war chest "
                                          f"(${war_chest:.0f} total)",
                                "portfolio_value": round(total_value, 2),
                            })
                else:
                    # BULL REBALANCE: normal rebalancing
                    for symbol in self.asset_symbols:
                        target_value = portfolio_value * self.allocations[symbol]
                        current_value = holdings[symbol] * row[symbol]
                        diff = target_value - current_value
                        if abs(diff) > 0.01:
                            holdings[symbol] += diff / row[symbol]
                            cash -= diff

                    target_cash = portfolio_value * self.allocations.get("CASH", 0)
                    cash = target_cash

            # Record equity (including war chest)
            total_eq = cash + war_chest
            for symbol in self.asset_symbols:
                total_eq += holdings[symbol] * row[symbol]
            equity_curve.append({"date": date, "equity": round(total_eq, 2)})

        # Final value
        final_row = prices_df.iloc[-1]
        final_value = cash + war_chest
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
                    bh_holdings[symbol] += (self.monthly_dca * self.allocations[symbol]) / row[symbol]
                cash_bh += self.monthly_dca * self.allocations.get("CASH", 0)
        bh_final = cash_bh
        for symbol in self.asset_symbols:
            bh_final += bh_holdings[symbol] * final_row[symbol]

        equity_df = pd.DataFrame(equity_curve)

        return {
            "mode": f"Bear Cash ({self.deploy_mode})",
            "deploy_mode": self.deploy_mode,
            "initial_capital": self.initial_capital,
            "total_invested": round(total_invested, 2),
            "final_value": round(final_value, 2),
            "total_return_pct": round((final_value - total_invested) / total_invested * 100, 2),
            "war_chest_remaining": round(war_chest, 2),
            "rebalance_count": sum(1 for t in trades if "TRIM" in t["action"]
                                   or "REBAL" in t.get("action", "")),
            "buyhold_final": round(bh_final, 2),
            "buyhold_return_pct": round((bh_final - total_invested) / total_invested * 100, 2),
            "beat_buyhold_pct": round((final_value - bh_final) / bh_final * 100, 2),
            "max_drawdown_pct": round(self._calc_max_drawdown(equity_df), 2),
            "equity_curve": equity_df,
            "trades": trades,
        }


def run_bear_cash_comparison(allocations: dict, initial_capital: float = 1000.0,
                             monthly_dca: float = 100.0, cache_dir: str = "data/",
                             start: str = None, end: str = None,
                             period: str = "2y") -> list:
    """Compare Bear Cash (both modes) vs Adaptive vs Fixed vs Buy&Hold."""
    from core.adaptive_rebalance import AdaptiveRebalanceEngine
    from core.rebalance import RebalanceEngine

    # Fetch macro
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
    macro_aligned = spy_close.reindex(prices.index, method="ffill")

    r = fixed.run(prices, rebalance_every_n=21)
    r["mode"] = "Fixed Monthly"
    results.append(r)

    # Adaptive (original)
    adaptive = AdaptiveRebalanceEngine(allocations, initial_capital, monthly_dca,
                                       bull_days=126, bear_days=21)
    r = adaptive.run_adaptive(prices, macro_aligned)
    results.append(r)

    # Bear Cash — proportional deploy
    bc_prop = BearCashEngine(allocations, initial_capital, monthly_dca,
                             deploy_mode="proportional", bull_days=126, bear_days=21)
    r = bc_prop.run_bear_cash(prices, macro_aligned)
    results.append(r)

    # Bear Cash — recovery deploy (more to biggest losers)
    bc_recov = BearCashEngine(allocations, initial_capital, monthly_dca,
                              deploy_mode="recovery", bull_days=126, bear_days=21)
    r = bc_recov.run_bear_cash(prices, macro_aligned)
    results.append(r)

    return results
