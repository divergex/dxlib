from typing import List

from dxlib.core import Portfolio
from dxlib.interfaces import TradingInterface, OrderInterface, AccountInterface, MarketInterface
from dxlib.market import Order, OrderTransaction, OrderEngine, Size, SizeType


class BacktestOrderInterface(OrderInterface):
    def __init__(self, context: "BacktestInterface"):
        super().__init__()
        self.context = context

    def send(self, orders: List[Order]):
        transactions = []
        for order in orders:
            if isinstance(order.quantity, Size) and order.quantity.is_relative:
                if order.quantity.kind == SizeType.PercentOfEquity:
                    equity = self.context.account_interface.equity()
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
        prices = self.context
        return 0


class BacktestMarketInterface(MarketInterface):
    pass



class BacktestInterface(TradingInterface):
    def __init__(self, history, history_view):
        self.history = history
        self.history_view = history_view
        self.portfolio = Portfolio()
        self.order_engine = OrderEngine()

        super().__init__(
            account_interface=BacktestAccountInterface(self),
            order_interface=BacktestOrderInterface(self),
        )
