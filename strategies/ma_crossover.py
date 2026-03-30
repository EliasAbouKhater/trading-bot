import pandas as pd
from strategies.base import Strategy


class MACrossover(Strategy):
    """Moving Average Crossover strategy.

    Buy when short SMA crosses above long SMA (golden cross).
    Sell when short SMA crosses below long SMA (death cross).
    """

    name = "ma_crossover"

    def __init__(self, config: dict):
        super().__init__(config)
        self.short_window = config.get("short_window", 20)
        self.long_window = config.get("long_window", 50)

    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["sma_short"] = df["Close"].rolling(window=self.short_window).mean()
        df["sma_long"] = df["Close"].rolling(window=self.long_window).mean()

        df["signal"] = 0
        # Golden cross: short crosses above long
        df.loc[(df["sma_short"] > df["sma_long"]) &
               (df["sma_short"].shift(1) <= df["sma_long"].shift(1)), "signal"] = 1
        # Death cross: short crosses below long
        df.loc[(df["sma_short"] < df["sma_long"]) &
               (df["sma_short"].shift(1) >= df["sma_long"].shift(1)), "signal"] = -1

        return df
