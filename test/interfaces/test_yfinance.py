import time
import unittest
from datetime import datetime

from dxlib.strategy.arbitrage.pairs.solver import generalized_arbitrage_signal
from dxlib.interfaces import MarketInterface
from dxlib.interfaces.external import yfinance

def convert_symbol(symbol):
    if '=' in symbol:
        base = symbol.replace('=X', '')
        if len(base) == 6:
            return f"{base[:3]}/{base[3:]}"
        else:
            return f"USD/{base}"
    return symbol

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
        symbol = ["EURUSD=X", "BRL=X", "EURBRL=X"]
        quotes = self.api.quote(symbol)
        quotes = quotes[["bid", "ask"]].reset_index(level='timestamp')
        quotes.index = quotes.index.map(convert_symbol)
        print(quotes)
        print(generalized_arbitrage_signal(quotes))

    def test_symbols(self):
        query = "totv"
        symbols = self.api.symbols(query)
        print(symbols)

if __name__ == '__main__':
    unittest.main()
