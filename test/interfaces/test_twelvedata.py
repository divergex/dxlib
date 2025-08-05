import unittest
from datetime import datetime

from dxlib.interfaces.external.twelvedata import TwelveData

def convert_symbol(symbol):
    if '=' in symbol:
        base = symbol.replace('=X', '')
        if len(base) == 6:
            return f"{base[:3]}/{base[3:]}"
        else:
            return f"USD/{base}"
    return symbol

class TestTwelveData(unittest.TestCase):
    def setUp(self):
        self.api = TwelveData("1f085232be684ab4b9c12c5235764443")
        self.api.start()

    def tearDown(self):
        self.api.stop()

    def test_historical(self):
        symbols = ["AAPL"]
        history = self.api.historical(symbols,
                                      datetime.strptime("2021-01-01", "%Y-%m-%d"),
                                      datetime.strptime("2021-02-01", "%Y-%m-%d"),
                                      "1min")
        print(history)

if __name__ == '__main__':
    unittest.main()
