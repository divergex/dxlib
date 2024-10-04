import unittest

import pandas as pd

from dxlib import History, HistorySchema, Executor, Strategy
from dxlib.strategies import RsiStrategy


class TestRsi(unittest.TestCase):
    def setUp(self):
        self.schema = HistorySchema(
            index={"security": str, "date": pd.Timestamp},
            columns={"close": float},
        )

        self.data = {
            "index": [
                ("AAPL", "2021-01-01"),
                ("MSFT", "2021-01-01"),
                ("AAPL", "2021-01-02"),
                ("MSFT", "2021-01-02"),
                ("GOOG", "2021-01-03"),
                ("AMZN", "2021-01-03"),
                ("FB", "2021-01-04"),
            ],
            "columns": ["close"],
            "data": [[100], [200], [101], [201], [102], [202], [103]],
            "index_names": ["security", "date"],
            "column_names": [""],
        }
        self.history = History(
            schema=self.schema,
            data=self.data
        )

    def test_rsi(self):
        output_schema = HistorySchema(
            index={"security": str, "date": pd.Timestamp},
            columns={"signal": int},
        )

        rsi = RsiStrategy(output_schema, 14, 70, 30)

        result = rsi.execute(self.history, self.history.get(index={"date": ["2021-01-01"]}))

        self.assertEqual(result.schema, output_schema)
        self.assertEqual([0, 0], [signal.value for signal in result.data["signal"].tolist()])
