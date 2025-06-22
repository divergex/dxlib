import unittest

import pandas as pd

from dxlib.core.signal import Signal
from dxlib.interfaces import MockMarket
from dxlib.strategy.signal import SignalStrategy
from dxlib.strategy.signal.custom.wick_reversal import WickReversal
from dxlib.strategy.views import SecuritySignalView


class TestRsi(unittest.TestCase):
    def setUp(self):
        mock = MockMarket()
        self.history = mock.historical(mock.securities(), n=10, random_seed=42)

    def test_rsi(self):
        wick = SignalStrategy(WickReversal(), history_view=SecuritySignalView(["close", "open", "high", "low"]))

        observation = self.history.data.index[-1]
        observation = self.history.loc(index=[observation])
        result = wick.execute(observation, self.history)
        data = result.data
        data = data.dropna()

        self.assertEqual(pd.DataFrame(data).shape[0], 1)
        self.assertEqual(Signal.HOLD, data.values[0, 0])
