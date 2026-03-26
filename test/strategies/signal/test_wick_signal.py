import unittest

import pandas as pd

from dxlib import OrderGenerator
from dxlib.core.signal import Signal
from dxlib.strategy.signal import SignalStrategy
from dxlib.strategy.signal.custom.wick_reversal import WickReversal
from dxlib.strategy.views import SecuritySignalView

from test.mock import MockHistory


class TestRsi(unittest.TestCase):
    def setUp(self):
        self.history = MockHistory.large_history()

    def test_rsi(self):
        signal = WickReversal()
        order_generator = OrderGenerator()
        signal_strategy = SignalStrategy(signal, order_generator)

        view = SecuritySignalView()

        observation = self.history.data.index[-1]
        observation = self.history.loc(index=[observation])
        result = signal_strategy.execute(observation, self.history, view)
        data = result.data
        data = data.dropna()

        self.assertEqual(pd.DataFrame(data).shape[0], 1)
        self.assertEqual(Signal.HOLD, data.values[0, 0])
