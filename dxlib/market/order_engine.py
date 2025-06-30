from typing import List

from dxlib import Portfolio, Security
from dxlib.market import OrderTransaction


class OrderEngine:
    def __init__(self, leg = None):
        self.default_leg = leg or Security("USD")

    def add(self, portfolio: Portfolio, transactions: List[OrderTransaction]):
        for transaction in transactions:
            portfolio.add(transaction.security, transaction.amount)
            portfolio.add(self.default_leg, -transaction.value)
        portfolio.drop_zero()
        return portfolio

    def to_portfolio(self, transactions: List[OrderTransaction]):
        # transform a list of transactions into additions into a portfolio
        portfolio = Portfolio()
        return self.add(portfolio, transactions)
