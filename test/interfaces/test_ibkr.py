import time
import unittest

from dxlib.interfaces import Ibkr


class TestIbkr(unittest.TestCase):
    def setUp(self):
        self.api = Ibkr("127.0.0.1", 4002, 0)
        self.api.start()

    def tearDown(self):
        self.api.stop()
        time.sleep(1)

    def test_historical(self):
        symbols = ["AAPL"]
        history = self.api.market_interface.historical(symbols, "2021-01-01", "2021-02-01", "D")
        print(history)

    def test_accounts(self):
        accounts = self.api.account_interface.accounts()
        print(accounts)



if __name__ == '__main__':
    unittest.main()
