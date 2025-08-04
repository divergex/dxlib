from typing import List

from dxlib.core import Portfolio, Instrument
from .limit_order import LimitOrderFactory
from .market_order import MarketOrderFactory
from ..transaction import OrderTransaction


class OrderEngine:
    def __init__(self, leg = None):
        self.default_leg = leg or Instrument("USD")

    market = MarketOrderFactory()
    limit = LimitOrderFactory()

    def record(self, portfolio: Portfolio, transactions: List[OrderTransaction]) -> Portfolio:
        for transaction in transactions:
            portfolio.add(transaction.order.instrument, transaction.amount)
            portfolio.add(self.default_leg, -transaction.value)
        portfolio.drop_zero()
        return portfolio

    def to_portfolio(self, transactions: List[OrderTransaction]):
        # transform a list of transactions into additions into a portfolio
        portfolio = Portfolio()
        return self.record(portfolio, transactions)
