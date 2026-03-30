import numpy as np
import pandas as pd
from strategies.base import Strategy


class GridTrading(Strategy):
    """Grid Trading strategy.

    Places buy orders at evenly spaced price levels below current price
    and sell orders above. Profits from price oscillation within a range.
    """

    name = "grid"

    def __init__(self, config: dict):
        super().__init__(config)
        self.grid_low = config.get("grid_low", 400)
        self.grid_high = config.get("grid_high", 600)
        self.num_grids = config.get("num_grids", 10)
        self.grid_levels = np.linspace(self.grid_low, self.grid_high, self.num_grids + 1)

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Grid signals: buy when price crosses below a grid level,
        sell when price crosses above a grid level."""
        df = df.copy()
        df["signal"] = 0

        prev_close = df["Close"].shift(1)

        for level in self.grid_levels:
            # Price dropped through grid level → buy
            buy_mask = (prev_close >= level) & (df["Close"] < level)
            df.loc[buy_mask, "signal"] = 1

            # Price rose through grid level → sell
            sell_mask = (prev_close <= level) & (df["Close"] > level)
            df.loc[sell_mask, "signal"] = df.loc[sell_mask, "signal"].where(
                df.loc[sell_mask, "signal"] == 1, -1
            )

        return df
