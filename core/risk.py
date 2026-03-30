class RiskManager:
    """Manages risk per trade, stop-loss, take-profit, and drawdown kill switch."""

    def __init__(self, config: dict):
        self.max_risk_pct = config.get("max_risk_per_trade_pct", 2.0)
        self.stop_loss_pct = config.get("stop_loss_pct", 5.0)
        self.take_profit_pct = config.get("take_profit_pct", 10.0)
        self.max_drawdown_pct = config.get("max_drawdown_pct", 15.0)
        self.max_positions = config.get("max_open_positions", 5)
        self.peak_equity = 0.0
        self.killed = False

    def check_drawdown(self, current_equity: float) -> bool:
        """Returns True if max drawdown exceeded (kill switch triggered)."""
        self.peak_equity = max(self.peak_equity, current_equity)
        if self.peak_equity == 0:
            return False
        drawdown_pct = (self.peak_equity - current_equity) / self.peak_equity * 100
        if drawdown_pct >= self.max_drawdown_pct:
            self.killed = True
        return self.killed

    def position_size(self, equity: float, entry_price: float) -> float:
        """Max dollar amount to allocate to this trade."""
        return equity * (self.max_risk_pct / 100)

    def shares_for_trade(self, equity: float, entry_price: float) -> float:
        """Number of (fractional) shares to buy."""
        dollars = self.position_size(equity, entry_price)
        return dollars / entry_price if entry_price > 0 else 0

    def stop_loss_price(self, entry_price: float, side: str = "long") -> float:
        factor = 1 - self.stop_loss_pct / 100 if side == "long" else 1 + self.stop_loss_pct / 100
        return entry_price * factor

    def take_profit_price(self, entry_price: float, side: str = "long") -> float:
        factor = 1 + self.take_profit_pct / 100 if side == "long" else 1 - self.take_profit_pct / 100
        return entry_price * factor

    def can_open_position(self, open_count: int) -> bool:
        return not self.killed and open_count < self.max_positions

    def reset(self):
        """Reset for new backtest run."""
        self.peak_equity = 0.0
        self.killed = False
