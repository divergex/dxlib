import unittest

import pandas as pd

from dxlib import Executor, Signal
from dxlib.strategies import RsiStrategy
from dxlib.interfaces import InvestingCom


class TestRsi(unittest.TestCase):
    def setUp(self):
        self.api = InvestingCom()

        self.output_schema = self.api.market_interface.history_schema()
        self.output_schema.columns = {"signal": int}

        self.end = pd.Timestamp("2021-01-01")
        self.start = self.end - pd.Timedelta(days=365)

        self.strategy = RsiStrategy(self.output_schema, 14, 70, 30)

    def test_run(self):
        executor = Executor(self.strategy)

        params = {"symbols": ["AAPL", "MSFT"], "from": self.start.timestamp(), "to": self.end.timestamp()}
        result = executor.run(self.api.market_interface.history(params))

        self.assertEqual(result.schema, self.output_schema)
        signal = result.get(index={"date": ["2020-01-22"], "instruments": ["AAPL"]})

        self.assertEqual(Signal.SELL, signal.data["signal"].values[0])
