import pandas as pd
import numpy as np
from core.risk import RiskManager
from strategies.base import Strategy


class BacktestEngine:
    """Simulates trading on historical data and computes performance metrics."""

    def __init__(self, strategy: Strategy, risk_manager: RiskManager,
                 initial_capital: float = 100.0):
        self.strategy = strategy
        self.risk = risk_manager
        self.initial_capital = initial_capital

    def run(self, df: pd.DataFrame) -> dict:
        """Run backtest and return metrics + trade log."""
        self.risk.reset()

        signals_df = self.strategy.generate_signals(df)
        signals_df = signals_df.dropna(subset=["signal"])

        cash = self.initial_capital
        shares = 0.0
        equity_curve = []
        trades = []
        entry_price = 0.0

        for date, row in signals_df.iterrows():
            price = row["Close"]
            signal = row["signal"]
            equity = cash + shares * price

            # Check kill switch
            if self.risk.check_drawdown(equity):
                # Liquidate everything
                if shares > 0:
                    cash += shares * price
                    trades.append({
                        "date": date, "action": "KILL_SELL",
                        "price": price, "shares": shares,
                        "pnl": (price - entry_price) * shares
                    })
                    shares = 0.0
                equity_curve.append({"date": date, "equity": cash})
                continue

            # Check stop-loss / take-profit on open position
            if shares > 0 and entry_price > 0:
                sl = self.risk.stop_loss_price(entry_price, "long")
                tp = self.risk.take_profit_price(entry_price, "long")
                if price <= sl:
                    cash += shares * price
                    trades.append({
                        "date": date, "action": "STOP_LOSS",
                        "price": price, "shares": shares,
                        "pnl": (price - entry_price) * shares
                    })
                    shares = 0.0
                    entry_price = 0.0
                elif price >= tp:
                    cash += shares * price
                    trades.append({
                        "date": date, "action": "TAKE_PROFIT",
                        "price": price, "shares": shares,
                        "pnl": (price - entry_price) * shares
                    })
                    shares = 0.0
                    entry_price = 0.0

            # Process signal
            if signal == 1 and shares == 0:  # Buy
                buy_shares = self.risk.shares_for_trade(equity, price)
                if buy_shares > 0 and cash >= buy_shares * price:
                    shares = buy_shares
                    cash -= shares * price
                    entry_price = price
                    trades.append({
                        "date": date, "action": "BUY",
                        "price": price, "shares": shares, "pnl": 0
                    })
            elif signal == -1 and shares > 0:  # Sell
                cash += shares * price
                pnl = (price - entry_price) * shares
                trades.append({
                    "date": date, "action": "SELL",
                    "price": price, "shares": shares, "pnl": pnl
                })
                shares = 0.0
                entry_price = 0.0

            equity = cash + shares * price
            equity_curve.append({"date": date, "equity": equity})

        # Final liquidation if still holding
        if shares > 0 and len(signals_df) > 0:
            last_price = signals_df.iloc[-1]["Close"]
            cash += shares * last_price
            pnl = (last_price - entry_price) * shares
            trades.append({
                "date": signals_df.index[-1], "action": "FINAL_SELL",
                "price": last_price, "shares": shares, "pnl": pnl
            })
            shares = 0.0

        final_equity = cash
        equity_df = pd.DataFrame(equity_curve)

        return {
            "strategy": self.strategy.name,
            "params": self.strategy.config,
            "initial_capital": self.initial_capital,
            "final_equity": round(final_equity, 2),
            "total_return_pct": round((final_equity - self.initial_capital) / self.initial_capital * 100, 2),
            "total_trades": len(trades),
            "winning_trades": sum(1 for t in trades if t["pnl"] > 0),
            "losing_trades": sum(1 for t in trades if t["pnl"] < 0),
            "win_rate_pct": round(sum(1 for t in trades if t["pnl"] > 0) / max(len([t for t in trades if t["pnl"] != 0]), 1) * 100, 1),
            "max_drawdown_pct": round(self._calc_max_drawdown(equity_df), 2) if not equity_df.empty else 0,
            "sharpe_ratio": round(self._calc_sharpe(equity_df), 2) if not equity_df.empty else 0,
            "trades": trades,
            "equity_curve": equity_df,
        }

    def _calc_max_drawdown(self, equity_df: pd.DataFrame) -> float:
        if equity_df.empty or "equity" not in equity_df.columns:
            return 0.0
        equity = equity_df["equity"]
        peak = equity.expanding().max()
        drawdown = (peak - equity) / peak * 100
        return drawdown.max()

    def _calc_sharpe(self, equity_df: pd.DataFrame, risk_free_rate: float = 0.04) -> float:
        if len(equity_df) < 2:
            return 0.0
        returns = equity_df["equity"].pct_change().dropna()
        if returns.std() == 0:
            return 0.0
        daily_rf = risk_free_rate / 252
        excess = returns.mean() - daily_rf
        return excess / returns.std() * np.sqrt(252)
