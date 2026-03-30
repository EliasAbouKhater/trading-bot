import pandas as pd
from strategies.base import Strategy


class PairsTrading(Strategy):
    """Pairs Trading strategy.

    Trades the spread between two correlated assets (e.g., SPY and QQQ).
    Enters when z-score of the spread exceeds threshold, exits when it reverts.
    """

    name = "pairs"

    def __init__(self, config: dict):
        super().__init__(config)
        self.symbol_a = config.get("symbol_a", "SPY")
        self.symbol_b = config.get("symbol_b", "QQQ")
        self.lookback = config.get("lookback", 60)
        self.entry_z = config.get("entry_z", 2.0)
        self.exit_z = config.get("exit_z", 0.5)

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expects DataFrame with '{symbol_a}_close' and '{symbol_b}_close' columns."""
        df = df.copy()
        col_a = f"{self.symbol_a}_close"
        col_b = f"{self.symbol_b}_close"

        if col_a not in df.columns or col_b not in df.columns:
            raise ValueError(f"Pairs trading requires columns: {col_a}, {col_b}. "
                             f"Use core.data.fetch_pair() to prepare data.")

        # Compute spread ratio and z-score
        df["spread"] = df[col_a] / df[col_b]
        df["spread_mean"] = df["spread"].rolling(window=self.lookback).mean()
        df["spread_std"] = df["spread"].rolling(window=self.lookback).std()
        df["z_score"] = (df["spread"] - df["spread_mean"]) / df["spread_std"]

        df["signal"] = 0
        # Spread too high → short A / long B (we simplify: sell signal on A)
        df.loc[df["z_score"] > self.entry_z, "signal"] = -1
        # Spread too low → long A / short B (buy signal on A)
        df.loc[df["z_score"] < -self.entry_z, "signal"] = 1
        # Revert to neutral when z-score crosses back inside exit band
        df.loc[df["z_score"].abs() < self.exit_z, "signal"] = 0

        return df
