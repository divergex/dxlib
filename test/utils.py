import pandas as pd

from dxlib import HistorySchema


class Mock:
    @property
    def schema(self) -> HistorySchema:
        return HistorySchema(
            index={"security": str, "date": pd.Timestamp},
            columns={"open": float, "close": float},
        )

    @property
    def tight_data(self) -> pd.DataFrame:
        return pd.DataFrame({
            "open": [100, 200, 101, 201, 102, 202, 103],
        }, index=pd.MultiIndex.from_tuples([
            ("AAPL", "2021-01-01"),
            ("MSFT", "2021-01-01"),
            ("AAPL", "2021-01-02"),
            ("MSFT", "2021-01-02"),
            ("GOOG", "2021-01-03"),
            ("AMZN", "2021-01-03"),
            ("FB", "2021-01-04"),
        ], names=["security", "date"]))

    @property
    def small_data(self) -> pd.DataFrame:
        return pd.DataFrame({
            "open": [100, 200],
        }, index=pd.MultiIndex.from_tuples([
            ("TSLA", "2021-01-01"),
            ("MSFT", "2021-01-01"),
        ], names=["security", "date"]))

    @property
    def large_data(self) -> pd.DataFrame:
        return pd.DataFrame({
            "open": [100, 200, 101, 201, 102, 202, 103, 104, 204, 105, 205, 106, 206, 107, 207, 108, 208],
            "close": [110, 210, 111, 211, 112, 212, 113, 114, 214, 115, 215, 116, 216, 117, 217, 118, 218],
        }, index=pd.MultiIndex.from_tuples([
            ("AAPL", "2021-01-01"),
            ("MSFT", "2021-01-01"),
            ("AAPL", "2021-01-02"),
            ("MSFT", "2021-01-02"),
            ("GOOG", "2021-01-03"),
            ("AMZN", "2021-01-03"),
            ("FB", "2021-01-04"),
            ("AAPL", "2021-01-05"),
            ("MSFT", "2021-01-05"),
            ("GOOG", "2021-01-06"),
            ("AMZN", "2021-01-06"),
            ("FB", "2021-01-07"),
            ("AAPL", "2021-01-08"),
            ("MSFT", "2021-01-08"),
            ("GOOG", "2021-01-09"),
            ("AMZN", "2021-01-09"),
            ("FB", "2021-01-10"),
        ], names=["security", "date"]))

    @property
    def stocks(self):
        return ["AAPL", "MSFT", "GOOG", "AMZN", "FB"]