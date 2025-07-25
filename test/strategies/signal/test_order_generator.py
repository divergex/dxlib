import unittest
from datetime import date

import pandas as pd

from dxlib import Signal, Security
from dxlib.strategy.order_generator.order_generator import OrderGenerator


class TestOrderGenerator(unittest.TestCase):
    def setUp(self) -> None:
        securities = [Security("AAPL"), Security("MSFT")]
        dates = [date(2025, 1, 1), date(2025, 1, 2)]
        self.signals = pd.DataFrame(
            {"signal": [Signal.BUY, Signal.SELL, Signal.SELL]},
            index=pd.MultiIndex.from_product([securities, dates], names=['instruments', 'date'])[:3],
        )

    def test_generate(self):
        print(self.signals)
        generator = OrderGenerator()
        orders = generator.generate(self.signals.reset_index('instruments'))
        print(orders)
