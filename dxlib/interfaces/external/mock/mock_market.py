import datetime

import pandas as pd
import numpy as np

from dxlib.history import History, HistorySchema
from dxlib.interfaces import MarketInterface


class MockMarket(MarketInterface):
    def securities(self):
        return ["AAPL", "MSFT", "PETR4.SA"]

    def historical(
            self,
            symbols: list[str],
            start: datetime = datetime.datetime(2021, 1, 1),
            end: datetime = datetime.datetime(2021, 1, 31),
            interval: str = "1d",
            n=100,
            random_seed: int = None
    ) -> History:
        columns = ["open", "high", "low", "close", "volume"]
        date = pd.date_range(start=start, end=end, periods=n)

        index = pd.MultiIndex.from_product([symbols, date], names=["instruments", "date"])

        data = pd.DataFrame(index=index, columns=columns)

        data.index.name = "date"

        if random_seed is not None:
            np.random.seed(random_seed)

        for column in columns:
            data[column] = np.random.rand(n * len(symbols))

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
