import unittest

import pandas as pd

from dxlib import History, HistorySchema, Executor, Strategy
from tests.mock_data import Mock


class TestExecutor(unittest.TestCase):
    def setUp(self):
        self.output_schema = HistorySchema(
            index={"security": str, "date": pd.Timestamp},
            columns={"open": float, "close": float},
        )

        class MyStrategy(Strategy):
            def execute(self, history: History, observation: History, *args, **kwargs) -> History:
                return history

        self.strategy = MyStrategy(self.output_schema)

        self.history = History(
            schema=Mock.schema,
            data=Mock.tight_data
        )

    def test_executor(self):
        executor = Executor(self.strategy)
        result = executor.run(self.history)

        self.assertEqual(result.schema, self.output_schema)
        self.assertEqual(result.data.index.names, self.output_schema.index.keys())
    