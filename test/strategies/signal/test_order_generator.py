import unittest
from datetime import date

import pandas as pd

import dxlib as dx
from dxlib import Signal, Instrument
from dxlib.strategy.order_generator.order_generator import OrderGenerator


class TestOrderGenerator(unittest.TestCase):
    def setUp(self) -> None:
        securities = [Instrument("AAPL"), Instrument("MSFT")]
        dates = [date(2025, 1, 1), date(2025, 1, 2)]
        self.signals = pd.DataFrame(
            {"signal": [Signal.BUY, Signal.SELL, Signal.SELL]},
            index=pd.MultiIndex.from_product([securities, dates], names=['instrument', 'date'])[:3],
        )
        self.history = dx.History(dx.HistorySchema(index={"date": date}, columns={"signal": Signal, "instrument": Instrument}),
                                  self.signals.reset_index("instrument"))

    def test_generate(self):
        print(self.signals)
        generator = OrderGenerator()
        orders = generator.generate(self.history)
        print(orders)
