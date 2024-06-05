import unittest

import pandas as pd

import dxlib as dx
from dxlib.strategies.custom_strategies import PairTradingStrategy


class TestRSIStrategy(unittest.TestCase):
    def setUp(self):
        self.security_manager = dx.SecurityManager.from_list(["AAPL", "MSFT"])
        self.aapl = self.security_manager["AAPL"]
        self.msft = self.security_manager["MSFT"]
        self.strategy = PairTradingStrategy(window=2, entry_z=1.0, exit_z=0.5, pairs=(self.aapl, self.msft))

    def test_execute(self):
        schema = dx.Schema(
            levels=[dx.SchemaLevel.DATE, dx.SchemaLevel.SECURITY],
            fields=["close"],
            security_manager=self.security_manager,
        )

        inventory = dx.Inventory({security: 0 for security in schema.security_manager.values()})

        history = dx.History(
            {
                (pd.Timestamp("2021-01-01"), self.aapl): {"close": 100},
                (pd.Timestamp("2021-01-01"), self.msft): {"close": 200},
                (pd.Timestamp("2021-01-02"), self.aapl): {"close": 110},
                (pd.Timestamp("2021-01-02"), self.msft): {"close": 210},
                (pd.Timestamp("2021-01-03"), self.aapl): {"close": 120},
                (pd.Timestamp("2021-01-03"), self.msft): {"close": 220},
                (pd.Timestamp("2021-01-04"), self.aapl): {"close": 130},
                (pd.Timestamp("2021-01-04"), self.msft): {"close": 230},
            },
            schema,
        )

        executor = dx.Executor(self.strategy, inventory)
        signals = executor.run(history)

        # date       security
        # 2021-01-01 AAPL (equity)  WAIT: None @ None
        #            MSFT (equity)  WAIT: None @ None
        # 2021-01-02 AAPL (equity)      SELL: 1 @ 110
        #            MSFT (equity)      SELL: 1 @ 210
        # 2021-01-03 AAPL (equity)      SELL: 1 @ 120
        #            MSFT (equity)      SELL: 1 @ 220
        print(signals)


if __name__ == "__main__":
    unittest.main()
