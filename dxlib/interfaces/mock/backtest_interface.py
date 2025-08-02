from typing import List, Optional

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
                price = self.context.market.quote(order.instrument).item()
                if order.quantity.kind == SizeType.PercentOfEquity:
                    equity = self.context.account.equity()
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
        prices = self.context.market.quote(portfolio.securities)
        return portfolio.value(prices)


class BacktestMarketInterface(MarketInterface):
    prices: pd.Series

    def __init__(self, context: "BacktestInterface", base_security: Optional[Instrument] = None):
        super().__init__()
        self.context = context
        self.index = None
        self.observation = None
        self.base_security = base_security or Instrument("USD")
        self.history = History()

        self.prices = pd.Series({self.base_security: 1}, name="price")
        self.price_history = History()

    def quote(self, security: str | Instrument | List[Instrument] | List[str]) -> pd.Series:
        if isinstance(security, List) and len(security) > 0 and isinstance(security[0], str):
            instruments = [Instrument(sec) for sec in security]
        elif isinstance(security, (Instrument, str)):
            instruments = [Instrument(security) if isinstance(security, str) else security]
        else:
            return pd.Series()
        return self.prices.loc[instruments]

    def history_schema(self) -> HistorySchema:
        return self.context.history.history_schema.copy()

    def get_view(self):
        for observation in self.context.history_view.iter(self.context.history):
            self.history.concat(observation)
            self.prices = self.prices.combine_first(prices := self.context.history_view.price(observation))
            self.prices.update(prices)
            self.price_history.concat(observation)
            yield observation

class BacktestInterface(TradingInterface):
    account: BacktestAccountInterface
    order: BacktestOrderInterface
    market: BacktestMarketInterface

    def __init__(self, history: History, history_view: HistoryView, portfolio: Optional[Portfolio] = None):
        self.history = history
        self.history_view = history_view
        self.portfolio = portfolio if portfolio is not None else Portfolio()
        self.order_engine = OrderEngine()

        super().__init__(
            account=BacktestAccountInterface(self),
            order=BacktestOrderInterface(self),
            market=BacktestMarketInterface(self)
        )

    def iter(self):
        return self.market.get_view()
