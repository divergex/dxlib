import random
import unittest

import pandas as pd

import dxlib as dx
from dxlib.strategy.signal import SignalStrategy
from dxlib.strategy.views import SecuritySignalView

from dxlib.strategy.signal.custom.wick_reversal import WickReversal

from test.mock import MockHistory


class TestRsi(unittest.TestCase):
    def setUp(self):
        random.seed(747)
        self.history = MockHistory.ohlcv_history()

    def test_rsi(self):
        signal = WickReversal()
        order_generator = dx.OrderGenerator()
        signal_strategy = SignalStrategy(signal, order_generator)

        view = SecuritySignalView()

        observation = self.history.data.index[-1]
        observation = self.history.loc(index=[observation])

        signal_strategy.validate(observation, view)
        result = signal_strategy.execute(observation, self.history, view)

        data = result.data
        data = data.dropna()

        self.assertEqual(pd.DataFrame(data).shape[0], 1)
        actual: dx.Order = data.values[0, 0]  # type: ignore
        self.assertEqual("MSFT", actual.instrument.symbol)
        self.assertEqual(dx.Side.SELL, actual.side)
        self.assertEqual(.05, actual.quantity.value)
