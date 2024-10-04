import unittest

from dxlib.interfaces import InvestingCom
from dxlib import History


class TestInvestingCom(unittest.TestCase):
    def setUp(self):
        self.api = InvestingCom()

    def test_history(self):
        params = {
            "symbols": "AAPL",
            "resolution": "D",
            "from": 1609459200,
            "to": 1612137600,
        }
        result = self.api.market_interface.history(params)
        self.assertIsInstance(result, History)
        self.assertEqual(20, len(result))
        # Check if
        self.assertEqual(["AAPL"], result.levels("security"))

    def test_multiple_symbols(self):
        params = {
            "symbols": ["AAPL", "MSFT"],
            "resolution": "D",
            "from": 1609459200,
            "to": 1612137600,
        }
        result = self.api.market_interface.history(params)
        self.assertIsInstance(result, History)
        self.assertEqual(40, len(result))
        # Check if
        self.assertEqual(["AAPL", "MSFT"], result.levels("security"))
