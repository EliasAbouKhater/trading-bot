from abc import ABC, abstractmethod
import pandas as pd


class Strategy(ABC):
    """Base class for all trading strategies.

    Subclasses must implement generate_signals() which adds a 'signal' column
    to the OHLCV DataFrame: 1 = buy, -1 = sell, 0 = hold.
    """

    name: str = "base"

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add 'signal' column to OHLCV DataFrame."""
        ...

    def describe(self) -> dict:
        return {"name": self.name, "params": self.config}
