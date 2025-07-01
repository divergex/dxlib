from typing import List

from dxlib.core import Portfolio
from dxlib.interfaces import TradingInterface, OrderInterface, AccountInterface
from dxlib.market import Order, OrderTransaction, OrderEngine


class BacktestOrderInterface(OrderInterface):
    def __init__(self, context: "BacktestInterface"):
        super().__init__()
        self.context = context

    def send(self, orders: List[Order]):
        transactions = []
        for order in orders:
            transactions.append(OrderTransaction(order))
        self.context.order_engine.trade(self.context.portfolio, transactions)
        return transactions


class BacktestAccountInterface(AccountInterface):
    def __init__(self, context: "BacktestInterface"):
        super().__init__()
        self.context = context

    def portfolio(self) -> Portfolio:
        return self.context.portfolio


class BacktestInterface(TradingInterface):
    def __init__(self):
        self.portfolio = Portfolio()
        self.order_engine = OrderEngine()

        super().__init__(
            account_interface=BacktestAccountInterface(self),
            order_interface=BacktestOrderInterface(self),
        )
