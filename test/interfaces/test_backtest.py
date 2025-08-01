import random
import unittest

from dxlib.interfaces.mock import BacktestInterface
from dxlib.market import Order, Side
from test.data import Mock

class TestBacktest(unittest.TestCase):
    def setUp(self):
        self.interface: BacktestInterface = BacktestInterface()

    def test_trade(self):
        securities = Mock.securities()
        orders = [Order(security, random.randint(10, 50), random.randint(1, 5), random.choice([Side.BUY, Side.SELL])) for security in securities]
        self.interface.order_interface.send(orders)

        print(self.interface.account_interface.portfolio())
