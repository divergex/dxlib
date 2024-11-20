import unittest

from dxlib.interfaces import Ibkr
from dxlib.interfaces.external.ibkr.ibkr import OrderType


class TestIbkr(unittest.TestCase):
    def test_historical(self):
        api = Ibkr()

        symbols = ["AAPL", "MSFT"]

        history = api.market_interface.historical(symbols, "2021-01-01", "2021-02-01", "D")
        print(history)

    def test_portfolio(self):
        api = Ibkr()

        api.market_interface.start()

        portfolio = api.market_interface.portfolio('')
        print(portfolio)

    def test_post_limit(self):
        api = Ibkr()

        api.market_interface.start()

        api.market_interface.place_order('AAPL', "BUY", 1, OrderType.LIMIT, 100)

        while not (api.market_interface.orders or api.market_interface.sent_orders):
            pass

        print(api.market_interface.orders or api.market_interface.sent_orders)
        api.market_interface.stop()

    def test_get_orders(self):
        api = Ibkr()

        api.market_interface.start()

        orders = api.market_interface.get_orders()
        print(orders)

if __name__ == '__main__':
    unittest.main()
