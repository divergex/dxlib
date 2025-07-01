from typing import List

from dxlib.core import Portfolio, Security
from .market_order import MarketOrderFactory
from ..transaction import OrderTransaction


class OrderEngine:
    def __init__(self, leg = None):
        self.default_leg = leg or Security("USD")

    market = MarketOrderFactory()

    def trade(self, portfolio: Portfolio, transactions: List[OrderTransaction]) -> Portfolio:
        for transaction in transactions:
            portfolio.add(transaction.security, transaction.amount)
            portfolio.add(self.default_leg, -transaction.value)
        portfolio.drop_zero()
        return portfolio

    def to_portfolio(self, transactions: List[OrderTransaction]):
        # transform a list of transactions into additions into a portfolio
        portfolio = Portfolio()
        return self.trade(portfolio, transactions)
