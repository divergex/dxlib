import unittest

import pandas as pd

from dxlib import History, HistorySchema, Executor, Strategy
from dxlib.interfaces import BacktestInterface
from dxlib.strategy.views import SecurityPriceView
from test.data import MockHistory


def output_schema():
    return HistorySchema(
        index={"instruments": str, "date": pd.Timestamp},
        columns={"open": float, "close": float},
    )

class TestExecutor(unittest.TestCase):
    def setUp(self):
        class MyStrategy(Strategy):
            def output_schema(self, history: HistorySchema) -> HistorySchema:
                return output_schema()

            def execute(self, history: History, observation: History, *args, **kwargs) -> History:
                return history

        self.strategy = MyStrategy()

        self.history = MockHistory.large_history()

    def test_executor(self):
        view = SecurityPriceView()
        interface = BacktestInterface(self.history, view)
        executor = Executor(self.strategy, interface)

        result, portfolio = executor.run(view, interface.iter())

        self.assertEqual(result.history_schema, output_schema())
        self.assertEqual(set(result.data.index.names), output_schema().index.keys())
