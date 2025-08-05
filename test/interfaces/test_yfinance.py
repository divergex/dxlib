import unittest
from datetime import datetime

from dxlib.interfaces.external.wikipedia.wikipedia import Wikipedia
from dxlib.interfaces import MarketInterface
from dxlib.interfaces.external.yfinance import yfinance

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
        self.api: MarketInterface = yfinance.YFinance()
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

    def test_symbols(self):
        query = "totv"
        symbols = self.api.symbols(query)
        print(symbols)

    def test_large_historical(self):
        symbols = Wikipedia().sp500()
        history = self.api.historical(symbols,
                                      datetime.strptime("2021-01-01", "%Y-%m-%d"),
                                      datetime.strptime("2021-02-01", "%Y-%m-%d"),
                                      "1d")
        print(history)

if __name__ == '__main__':
    unittest.main()
