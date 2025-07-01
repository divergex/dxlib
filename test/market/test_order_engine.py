import unittest

from dxlib import Security
from dxlib.market import OrderTransaction, Order, Side
from dxlib.market.orders.order_engine import OrderEngine


class TestOrderEngine(unittest.TestCase):
    def setUp(self):
        securities = [
            Security("MSFT"),
            Security("AAPL"),
            Security("PETR4.SA"),
        ]
        orders = [
            Order(securities[0], 100, 20, Side.BUY)
        ]
        transactions = [
            OrderTransaction(orders[0], 101, 10),
        ]

        self.securities = securities
        self.orders = orders
        self.transactions = transactions

    def test_order_engine(self):
        engine = OrderEngine()
        print(
            engine.to_portfolio(self.transactions)
        )
