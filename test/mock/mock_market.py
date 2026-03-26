from datetime import datetime

import pandas as pd
import numpy as np

from dxlib.history import History, HistorySchema
from dxlib.interfaces import MarketInterface


class MockMarket(MarketInterface):
    def __init__(self):
        self.seed = None
        self.rows = None

    def setup(self, seed, rows):
        self.seed = seed
        self.rows = rows
        np.random.seed(self.seed)

    def historical(
            self,
            symbols: list[str],
            start: datetime = datetime(2021, 1, 1),
            end: datetime = datetime(2021, 1, 31),
            interval: str = "1d",
            **kwargs
    ) -> History:
        assert self.rows is not None
        date = pd.date_range(start=start, end=end, periods=n)
        index = pd.MultiIndex.from_product([symbols, date], names=["instrument", "date"])
        columns = ["open", "high", "low", "close", "volume"]

        data = pd.DataFrame(index=index, columns=columns)

        data.index.name = "date"

        for column in columns:
            data[column] = np.random.rand(self.rows * len(symbols))

        return History(
            history_schema=self.history_schema,
            data=data
        )

    @property
    def history_schema(self):
        return HistorySchema(
            index={"date": pd.Timestamp, "instruments": str},
            columns={"open": float, "high": float, "low": float, "close": float, "volume": float}
        )
