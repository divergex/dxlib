import time
import unittest
from datetime import datetime


from dxlib.interfaces import MarketInterface
from dxlib.interfaces.external import yfinance


class TestYFinance(unittest.TestCase):
    def setUp(self):
        self.api: MarketInterface = yfinance.YFinance("d=AQABBKAePWgCEKDPPXmKO5K9lLem9_ddGqcFEgEBAQFwPmhHaB6kxyMA_eMCAA&S=AQAAAlfJsDQw5RqBNL49p1OV1Eg")
        self.api.start()

    def tearDown(self):
        self.api.stop()

    def test_historical(self):
        symbols = ["EURUSD=X"]
        history = self.api.historical(symbols,
                                      datetime.strptime("2021-01-01", "%Y-%m-%d"),
                                      datetime.strptime("2021-02-01", "%Y-%m-%d"),
                                      "1d")
        print(history)

    def test_quote(self):
        symbol = ["EURUSD=X", "BRL=X", "BRLEUR=X"]
        wtf = self.api.quote(symbol)
        print(wtf[["bid", "ask"]])

if __name__ == '__main__':
    unittest.main()
