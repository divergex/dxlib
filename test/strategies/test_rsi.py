import unittest

import pandas as pd

from dxlib import HistorySchema, Executor
from dxlib.interfaces import MockMarket
from dxlib.strategy.custom import RsiStrategy


class TestRsi(unittest.TestCase):
    def setUp(self):
        self.history = MockMarket().historical(n=10, random_seed=42)

    def test_rsi(self):
        output_schema = HistorySchema(
            index={"date": pd.Timestamp},
            columns={"signal": int},
        )

        rsi = RsiStrategy(output_schema, 2, 70, 30)

        result = rsi.execute(history=self.history)
        # remove nans
        data = result.data
        data = data.dropna()

        self.assertEqual(result.history_schema, output_schema)
        self.assertEqual([0, 1, 1, -1, -1, 1, 1, 1, -1, -1], [signal.value for signal in data["signal"].tolist()])
