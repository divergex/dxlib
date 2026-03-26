from abc import ABC
from numbers import Number
import pandas as pd
import random
from datetime import timedelta, date

from dxlib import History, HistorySchema, Instrument

from .mock_instrument import MockInstrument

# TODO: Simulators
def generate_ohlcv_data(store, start_date, end_date, num_points):
    tickers = list(store.keys())

    # Generate random dates within the interval
    date_range = (end_date - start_date).days
    dates = [
        (start_date + timedelta(days=random.randint(0, date_range))).strftime("%Y-%m-%d")
        for _ in range(num_points)
    ]

    index = [(store[random.choice(tickers)], date) for date in dates]

    def random_ohlcv():
        open_ = round(random.uniform(100, 500), 2)
        close = round(random.uniform(open_ * 0.95, open_ * 1.05), 2)
        high = round(max(open_, close) * random.uniform(1.0, 1.05), 2)
        low = round(min(open_, close) * random.uniform(0.95, 1.0), 2)
        volume = random.randint(1_000_000, 50_000_000)
        return [open_, high, low, close, volume]

    return {
        "index": index,
        "index_names": ["instrument", "date"],
        "columns": ["open", "high", "low", "close", "volume"],
        "column_names": [""],
        "data": [random_ohlcv() for _ in range(num_points)],
    }

class MockHistory(ABC):
    columns = ["open", "close"]

    @classmethod
    def schema(cls) -> HistorySchema:
        return HistorySchema(
            index={"instrument": Instrument, "date": pd.Timestamp},
            columns={"open": Number},
        )

    @classmethod
    def large_schema(cls) -> HistorySchema:
        return HistorySchema(
            index={"instrument": Instrument, "date": pd.Timestamp},
            columns={"open": Number, "volume": Number},
        )

    @classmethod
    def tight_data(cls) -> dict:
        store = MockInstrument.store()
        return {
            "index": [
                (store["AAPL"], "2021-01-01"),
                (store["MSFT"], "2021-01-01"),
                (store["AAPL"], "2021-01-02"),
                (store["MSFT"], "2021-01-02"),
                (store["GOOG"], "2021-01-03"),
                (store["AMZN"], "2021-01-03"),
                (store["FB"], "2021-01-04"),
            ],
            "columns": ["open"],
            "data": [[100], [200], [101], [201], [102], [202], [103]],
            "index_names": ["instrument", "date"],
            "column_names": [""],
        }

    @classmethod
    def small_data(cls) -> dict:
        store = MockInstrument.store()
        return {"index": [
            (store["TSLA"], "2021-01-01"),
            (store["MSFT"], "2021-01-01"),
        ],
            "columns": ["open"],
            "data": [[100], [200]],
            "index_names": ["instrument", "date"],
            "column_names": [""]
        }

    @classmethod
    def large_data(cls) -> dict:
        store = MockInstrument.store()

        return {
            "index": [
                (store["AAPL"], "2021-01-01"),
                (store["MSFT"], "2021-01-01"),
                (store["AAPL"], "2021-01-02"),
                (store["MSFT"], "2021-01-02"),
                (store["GOOG"], "2021-01-03"),
                (store["AMZN"], "2021-01-03"),
                (store["FB"], "2021-01-04"),
                (store["AAPL"], "2021-01-05"),
                (store["MSFT"], "2021-01-05"),
                (store["GOOG"], "2021-01-06"),
                (store["AMZN"], "2021-01-06"),
                (store["FB"], "2021-01-07"),
                (store["AAPL"], "2021-01-08"),
                (store["MSFT"], "2021-01-08"),
                (store["GOOG"], "2021-01-09"),
                (store["AMZN"], "2021-01-09"),
                (store["FB"], "2021-01-10"),
            ],
            "columns": ["open", "volume"],
            "data": [
                [100, 1000],
                [200, 2000],
                [101, 1001],
                [201, 2001],
                [102, 1002],
                [202, 2002],
                [103, 1003],
                [203, 2003],
                [104, 1004],
                [204, 2004],
                [105, 1005],
                [205, 2005],
                [106, 1006],
                [206, 2006],
                [107, 1007],
                [207, 2007],
                [108, 1008],
            ],
            "index_names": ["instrument", "date"],
            "column_names": [""],
        }

    @classmethod
    def ohlcv_data(cls, start_date=None, end_date=None, num_points=16) -> dict:
        store = MockInstrument.store()
        if start_date is None:
            start_date = date(2021, 1, 1)
        if end_date is None:
            end_date = date(2021, 1, 7)
        return generate_ohlcv_data(store, start_date, end_date, num_points)

    @classmethod
    def large_history(cls) -> History:
        data = cls.large_data()
        schema = cls.large_schema()

        return History(schema, data)

    @classmethod
    def history(cls) -> History:
        data = cls.tight_data()
        schema = cls.schema()
        return History(schema, data)

    @classmethod
    def ohlcv_history(cls, start_date=None, end_date=None, num_points=16) -> History:
        data = cls.ohlcv_data(start_date, end_date, num_points)
        schema = cls.schema()
        schema.columns = {name: float for name in ["open", "high", "low", "close"]} | {"volume": int}

        return History(schema, data)
