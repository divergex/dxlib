import time
import unittest
from datetime import datetime


from dxlib.interfaces import MarketInterface
from dxlib.interfaces.external import yfinance


class TestYFinance(unittest.TestCase):
    def setUp(self):
        self.api: MarketInterface = yfinance.YFinance()

    def tearDown(self):
        time.sleep(1)

    def test_historical(self):
        symbols = ["EURUSD=X"]
        history = self.api.historical(symbols,
                                      datetime.strptime("2021-01-01", "%Y-%m-%d"),
                                      datetime.strptime("2021-02-01", "%Y-%m-%d"),
                                      "1d")
        print(history)


if __name__ == '__main__':
    unittest.main()
