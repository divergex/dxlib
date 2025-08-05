import unittest

import pandas as pd

from dxlib import HistorySchema
from dxlib.core.indicators import RSI
from dxlib.core.signal import Signal
from dxlib.interfaces import MockMarket
from dxlib.strategy.strategy import SignalStrategy


class TestRsi(unittest.TestCase):
    def setUp(self):
        self.history = MockMarket().historical(n=10, random_seed=42)

    def test_rsi(self):
        output_schema = HistorySchema(
            index={"date": pd.Timestamp},
            columns={"signal": int},
        )

        rsi = SignalStrategy(output_schema, RSI(window=3, upper=70, lower=30))

        # get last index
        observation = self.history.data.index[-1]
        observation = self.history.get(index={"date": [observation]})
        result = rsi.execute(,,
        data = result.data
        data = data.dropna()

        self.assertEqual(pd.DataFrame(data).shape[0], 1)
        self.assertEqual(Signal.HOLD, data.values[0, 0])
