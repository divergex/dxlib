from abc import ABC
from numbers import Number
import pandas as pd

from dxlib import HistorySchema, Instrument, InstrumentStore


class Mock(ABC):
    columns = ["open", "close"]
    stocks = ["AAPL", "MSFT", "GOOG", "AMZN", "FB"]

    @classmethod
    def instrument(cls):
        return [Instrument(symbol) for symbol in cls.stocks]

    @classmethod
    def schema(cls):
        return HistorySchema(
            index={"instrument": Instrument, "date": pd.Timestamp},
            columns={"open": Number},
        )

    @classmethod
    def large_schema(cls):
        return HistorySchema(
            index={"instrument": Instrument, "date": pd.Timestamp},
            columns={"open": Number, "volume": Number},
        )

    @classmethod
    def tight_data(cls):
        store = InstrumentStore.from_list(cls.instrument())
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
    def small_data(cls):
        store = InstrumentStore.from_list(cls.instrument())
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
    def large_data(cls):
        store = InstrumentStore.from_list(cls.instrument())

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
