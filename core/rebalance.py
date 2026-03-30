"""Portfolio rebalancing engine.

Strategy: Maintain fixed allocation ratios across multiple asset classes.
When any asset drifts from its target, sell the excess and buy the laggard.
Add monthly DCA on top.
"""

import pandas as pd
import numpy as np
from core.data import fetch_data


class RebalanceEngine:

    def __init__(self, allocations: dict, initial_capital: float = 1000.0,
                 monthly_dca: float = 100.0, rebalance_threshold_pct: float = 0.0):
        """
        allocations: {"PAXG/USD": 0.25, "BTC/USD": 0.25, "XRP/USD": 0.25, "CASH": 0.25}
        rebalance_threshold_pct: only rebalance if any asset drifts more than this %
                                 from target (0 = always rebalance on schedule)
        """
        self.allocations = allocations
        self.initial_capital = initial_capital
        self.monthly_dca = monthly_dca
        self.threshold = rebalance_threshold_pct
        self.asset_symbols = [s for s in allocations if s != "CASH"]

    def fetch_all_data(self, period: str = "2y", interval: str = "1d",
                       cache_dir: str = "data/",
                       start: str = None, end: str = None) -> pd.DataFrame:
        """Fetch and align all asset price data into one DataFrame."""
        frames = {}
        for symbol in self.asset_symbols:
            df = fetch_data(symbol, period, interval, cache_dir,
                            cache_max_hours=24, start=start, end=end)
            # Normalize index to date-only (strips timezone differences)
            series = df["Close"].copy()
            series.index = pd.to_datetime(series.index, utc=True).normalize().tz_localize(None)
            # Drop duplicate dates (keep last)
            series = series[~series.index.duplicated(keep="last")]
            frames[symbol] = series

        merged = pd.DataFrame(frames).dropna()
        return merged

    def run(self, prices_df: pd.DataFrame, rebalance_every_n: int = 1) -> dict:
        """
        Run rebalancing backtest.

        rebalance_every_n: rebalance every N bars (1=daily, 2=every 2 days,
                           5=weekly, 21=monthly, 63=quarterly, 252=yearly)
        """
        cash = self.initial_capital * self.allocations.get("CASH", 0)
        # Initial buy: allocate capital to each asset
        holdings = {}  # symbol -> number of units held
        for symbol in self.asset_symbols:
            target_dollars = self.initial_capital * self.allocations[symbol]
            price = prices_df[symbol].iloc[0]
            holdings[symbol] = target_dollars / price

        # Track
        equity_curve = []
        rebalance_log = []
        trades = []
        last_dca_month = None
        bars_since_rebalance = 0

        for i, (date, row) in enumerate(prices_df.iterrows()):
            bars_since_rebalance += 1

            # Calculate current portfolio value
            portfolio_value = cash
            for symbol in self.asset_symbols:
                portfolio_value += holdings[symbol] * row[symbol]

            # Monthly DCA: add cash on first bar of each month
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
                    "detail": f"+${self.monthly_dca:.0f} cash added",
                    "portfolio_value": round(portfolio_value, 2),
                })

            # Rebalance check
            if bars_since_rebalance >= rebalance_every_n and i > 0:
                bars_since_rebalance = 0

                # Current allocation percentages
                current_alloc = {}
                for symbol in self.asset_symbols:
                    current_alloc[symbol] = (holdings[symbol] * row[symbol]) / portfolio_value
                current_alloc["CASH"] = cash / portfolio_value

                # Check if any asset drifts beyond threshold
                needs_rebalance = False
                for key, target in self.allocations.items():
                    actual = current_alloc.get(key, 0)
                    if abs(actual - target) * 100 > self.threshold:
                        needs_rebalance = True
                        break

                if needs_rebalance:
                    # Rebalance: adjust all positions to target
                    rebalance_detail = []
                    for symbol in self.asset_symbols:
                        target_value = portfolio_value * self.allocations[symbol]
                        current_value = holdings[symbol] * row[symbol]
                        diff = target_value - current_value

                        if abs(diff) > 0.01:  # skip dust
                            units_change = diff / row[symbol]
                            old_units = holdings[symbol]
                            holdings[symbol] += units_change
                            cash -= diff  # positive diff = bought more = less cash
                            action = "BUY" if diff > 0 else "SELL"
                            rebalance_detail.append(
                                f"{action} {abs(units_change):.6f} {symbol} (${abs(diff):.2f})"
                            )

                    # Also rebalance cash
                    target_cash = portfolio_value * self.allocations.get("CASH", 0)
                    cash = target_cash

                    if rebalance_detail:
                        rebalance_log.append({
                            "date": date,
                            "portfolio_value": round(portfolio_value, 2),
                            "detail": "; ".join(rebalance_detail),
                        })
                        trades.append({
                            "date": date, "action": "REBALANCE",
                            "detail": "; ".join(rebalance_detail),
                            "portfolio_value": round(portfolio_value, 2),
                        })

            # Record equity
            equity_curve.append({"date": date, "equity": round(portfolio_value, 2)})

        # Final portfolio value
        final_row = prices_df.iloc[-1]
        final_value = cash
        for symbol in self.asset_symbols:
            final_value += holdings[symbol] * final_row[symbol]

        # Calculate total cash invested (initial + all DCA)
        months = len(set((d.year if hasattr(d, 'year') else pd.Timestamp(d).year,
                          d.month if hasattr(d, 'month') else pd.Timestamp(d).month)
                         for d in prices_df.index)) - 1  # exclude first month
        total_invested = self.initial_capital + (months * self.monthly_dca)

        # Buy-and-hold benchmark (same allocation, no rebalancing)
        bh_value = cash_bh = self.initial_capital * self.allocations.get("CASH", 0)
        bh_holdings = {}
        for symbol in self.asset_symbols:
            target_dollars = self.initial_capital * self.allocations[symbol]
            bh_holdings[symbol] = target_dollars / prices_df[symbol].iloc[0]

        # Add DCA to buy-and-hold too (proportional)
        last_m = None
        for date, row in prices_df.iterrows():
            m_key = (date.year if hasattr(date, 'year') else pd.Timestamp(date).year,
                     date.month if hasattr(date, 'month') else pd.Timestamp(date).month)
            if last_m is None:
                last_m = m_key
            elif m_key != last_m:
                last_m = m_key
                for symbol in self.asset_symbols:
                    dca_per_asset = self.monthly_dca * self.allocations[symbol]
                    bh_holdings[symbol] += dca_per_asset / row[symbol]
                cash_bh += self.monthly_dca * self.allocations.get("CASH", 0)

        bh_final = cash_bh
        for symbol in self.asset_symbols:
            bh_final += bh_holdings[symbol] * final_row[symbol]

        equity_df = pd.DataFrame(equity_curve)

        return {
            "rebalance_every_n": rebalance_every_n,
            "initial_capital": self.initial_capital,
            "total_invested": round(total_invested, 2),
            "final_value": round(final_value, 2),
            "total_return_pct": round((final_value - total_invested) / total_invested * 100, 2),
            "profit": round(final_value - total_invested, 2),
            "rebalance_count": len(rebalance_log),
            "buyhold_final": round(bh_final, 2),
            "buyhold_return_pct": round((bh_final - total_invested) / total_invested * 100, 2),
            "beat_buyhold_pct": round(
                (final_value - bh_final) / bh_final * 100, 2
            ),
            "max_drawdown_pct": round(self._calc_max_drawdown(equity_df), 2),
            "equity_curve": equity_df,
            "trades": trades,
            "rebalance_log": rebalance_log,
        }

    def _calc_max_drawdown(self, equity_df: pd.DataFrame) -> float:
        if equity_df.empty:
            return 0.0
        equity = equity_df["equity"]
        peak = equity.expanding().max()
        drawdown = (peak - equity) / peak * 100
        return drawdown.max()


def run_frequency_comparison(allocations: dict, initial_capital: float = 1000.0,
                             monthly_dca: float = 100.0, period: str = "2y",
                             cache_dir: str = "data/",
                             start: str = None, end: str = None) -> list:
    """Run rebalancing at multiple frequencies and compare."""
    engine = RebalanceEngine(allocations, initial_capital, monthly_dca)
    prices = engine.fetch_all_data(period=period, interval="1d",
                                   cache_dir=cache_dir, start=start, end=end)

    frequencies = [
        (1, "Daily"),
        (2, "Every 2 days"),
        (5, "Weekly"),
        (21, "Monthly"),
        (63, "Quarterly"),
        (126, "Semi-annual"),
        (252, "Yearly"),
    ]

    results = []
    for n, label in frequencies:
        if n > len(prices):
            continue
        r = engine.run(prices, rebalance_every_n=n)
        r["frequency_label"] = label
        results.append(r)

    return results
