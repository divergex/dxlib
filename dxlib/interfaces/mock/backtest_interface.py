from typing import List

import pandas as pd

from dxlib.core import Portfolio, Instrument
from dxlib.history import History, HistorySchema, HistoryView
from dxlib.interfaces import TradingInterface, OrderInterface, AccountInterface, MarketInterface
from dxlib.market import Order, OrderTransaction, OrderEngine, Size, SizeType


class BacktestOrderInterface(OrderInterface):
    def __init__(self, context: "BacktestInterface"):
        super().__init__()
        self.context = context

    def send(self, orders: List[Order]):
        transactions = []
        for order in orders:
            if order.is_none():
                continue
            if isinstance(order.quantity, Size) and order.quantity.is_relative:
                price = self.context.market_interface.quote(order.instrument).item()
                if order.quantity.kind == SizeType.PercentOfEquity:
                    equity = self.context.account_interface.equity()
                    quantity = order.quantity.value * equity / price
                elif order.quantity.kind == SizeType.PercentOfPosition:
                    raise NotImplementedError
                else:
                    raise TypeError("Invalid quantity type.")
                transactions.append(OrderTransaction(order, price, quantity))
            else:
                transactions.append(OrderTransaction(order, order.price, float(order.quantity)))

        self.context.order_engine.trade(self.context.portfolio, transactions)
        return transactions


class BacktestAccountInterface(AccountInterface):
    def __init__(self, context: "BacktestInterface"):
        super().__init__()
        self.context = context

    def portfolio(self) -> Portfolio:
        return self.context.portfolio

    def equity(self, *args, **kwargs) -> float:
        portfolio = self.context.portfolio
        prices = self.context.market_interface.quote(portfolio.securities)
        return portfolio.value(prices)


class BacktestMarketInterface(MarketInterface):
    def __init__(self, context: "BacktestInterface", base_security: Instrument = None):
        super().__init__()
        self.context = context
        self.index = None
        self.observation = None
        self.base_security = base_security or Instrument("USD")
        self.prices = pd.Series({self.base_security: 1}, name="price")
        self.history = History()

    def quote(self, security: str | Instrument | List[str | Instrument]) -> float | pd.Series:
        if isinstance(security, List) and len(security) > 0:
            if isinstance(security[0], str):
                security = [Instrument(sec) for sec in security]
        elif isinstance(security, (Instrument, str)):
            security = [Instrument(security) if isinstance(security, str) else security]
        return self.prices[security]

    def history_schema(self) -> HistorySchema:
        return self.context.history.history_schema.copy()

class BacktestInterface(TradingInterface):
    def __init__(self, history: History, history_view: HistoryView, portfolio: Portfolio = None):
        self.history = history
        self.portfolio = portfolio or Portfolio()
        self.order_engine = OrderEngine()
        self.pricing = history_view
        assert hasattr(self.pricing, "price"), "Provided `pricing: HistoryView` must contain `price` method if used for backtesting."

        super().__init__(
            account_interface=BacktestAccountInterface(self),
            order_interface=BacktestOrderInterface(self),
            market_interface=BacktestMarketInterface(self)
        )
