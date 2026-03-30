"""Alpaca broker wrapper for paper and live trading."""

import os
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    GetOrdersRequest,
)
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockLatestBarRequest, CryptoLatestBarRequest


def is_crypto(symbol: str) -> bool:
    """Check if symbol is a crypto pair (contains / or ends with USD)."""
    return "/" in symbol or symbol.endswith("USD")


class AlpacaBroker:
    """Thin wrapper around Alpaca's trading API."""

    def __init__(self, api_key: str = None, secret_key: str = None, paper: bool = True):
        self.api_key = api_key or os.environ.get("APCA_API_KEY_ID", "")
        self.secret_key = secret_key or os.environ.get("APCA_API_SECRET_KEY", "")

        if not self.api_key or not self.secret_key:
            raise ValueError("Alpaca API keys not set. Check .env or environment variables.")

        self.trading = TradingClient(self.api_key, self.secret_key, paper=paper)
        self.stock_data = StockHistoricalDataClient(self.api_key, self.secret_key)
        self.crypto_data = CryptoHistoricalDataClient(self.api_key, self.secret_key)

    def get_account(self) -> dict:
        """Get account info (equity, cash, buying power)."""
        acct = self.trading.get_account()
        return {
            "equity": float(acct.equity),
            "cash": float(acct.cash),
            "buying_power": float(acct.buying_power),
            "currency": acct.currency,
            "status": acct.status.value if hasattr(acct.status, 'value') else str(acct.status),
        }

    def get_positions(self) -> list:
        """Get all open positions."""
        positions = self.trading.get_all_positions()
        return [{
            "symbol": p.symbol,
            "qty": float(p.qty),
            "avg_entry": float(p.avg_entry_price),
            "current_price": float(p.current_price),
            "market_value": float(p.market_value),
            "unrealized_pnl": float(p.unrealized_pl),
            "unrealized_pnl_pct": float(p.unrealized_plpc) * 100,
        } for p in positions]

    def get_latest_price(self, symbol: str) -> float:
        """Get the latest bar close price for a symbol."""
        if is_crypto(symbol):
            req = CryptoLatestBarRequest(symbol_or_symbols=symbol)
            bars = self.crypto_data.get_crypto_latest_bar(req)
        else:
            req = StockLatestBarRequest(symbol_or_symbols=symbol)
            bars = self.stock_data.get_stock_latest_bar(req)
        return float(bars[symbol].close)

    def buy(self, symbol: str, qty: float = None, notional: float = None) -> dict:
        """Place a market buy order. Use qty for shares or notional for dollar amount."""
        tif = TimeInForce.GTC if is_crypto(symbol) else TimeInForce.DAY
        if qty:
            order_data = MarketOrderRequest(
                symbol=symbol, qty=qty,
                side=OrderSide.BUY, time_in_force=tif,
            )
        elif notional:
            order_data = MarketOrderRequest(
                symbol=symbol, notional=notional,
                side=OrderSide.BUY, time_in_force=tif,
            )
        else:
            raise ValueError("Must specify qty or notional")

        order = self.trading.submit_order(order_data)
        return self._order_dict(order)

    def sell(self, symbol: str, qty: float = None, notional: float = None) -> dict:
        """Place a market sell order."""
        tif = TimeInForce.GTC if is_crypto(symbol) else TimeInForce.DAY
        if qty:
            order_data = MarketOrderRequest(
                symbol=symbol, qty=qty,
                side=OrderSide.SELL, time_in_force=tif,
            )
        elif notional:
            order_data = MarketOrderRequest(
                symbol=symbol, notional=notional,
                side=OrderSide.SELL, time_in_force=tif,
            )
        else:
            raise ValueError("Must specify qty or notional")

        order = self.trading.submit_order(order_data)
        return self._order_dict(order)

    def get_orders(self, status: str = "open") -> list:
        """Get orders by status (open, closed, all)."""
        status_map = {
            "open": QueryOrderStatus.OPEN,
            "closed": QueryOrderStatus.CLOSED,
            "all": QueryOrderStatus.ALL,
        }
        req = GetOrdersRequest(status=status_map.get(status, QueryOrderStatus.OPEN))
        orders = self.trading.get_orders(req)
        return [self._order_dict(o) for o in orders]

    def close_position(self, symbol: str) -> dict:
        """Close entire position for a symbol."""
        order = self.trading.close_position(symbol)
        return self._order_dict(order)

    def close_all_positions(self) -> list:
        """Close all open positions."""
        responses = self.trading.close_all_positions()
        return [{"symbol": r.symbol, "status": str(r.status)} for r in responses]

    def _order_dict(self, order) -> dict:
        return {
            "id": str(order.id),
            "symbol": order.symbol,
            "side": order.side.value if hasattr(order.side, 'value') else str(order.side),
            "qty": str(order.qty) if order.qty else None,
            "notional": str(order.notional) if order.notional else None,
            "status": order.status.value if hasattr(order.status, 'value') else str(order.status),
            "created_at": str(order.created_at),
        }
