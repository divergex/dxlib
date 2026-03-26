from datetime import date
import unittest
from numbers import Number

import pandas as pd

from dxlib import Portfolio, HistorySchema, History, Instrument
from dxlib.core.portfolio import PortfolioHistory


class TestPortfolio(unittest.TestCase):
    def test_portfolio(self):
        p = Portfolio({Instrument("AAPL"): 2})
        print(p)

    def test_value(self):
        sec = [Instrument("AAPL"), Instrument("MSFT")]
        p = Portfolio({sec[0]: 2, sec[1]: 2})
        prices = pd.Series({sec[0]: 100, sec[1]: 250})

        print(p.value(prices))


class TestPortfolioHistory(unittest.TestCase):
    def test_history(self):
        sec = [Instrument("AAPL"), Instrument("MSFT")]
        ph2 = PortfolioHistory(
            schema_index={"instrument": Instrument, "date": date},
            data={
                'index': [(sec[0], date(2021, 1, 1)), (sec[1], date(2021, 1, 1))],
                'columns': ["quantity"],
                'data': [[2], [2]],
                'index_names': ["instrument", "date"],
                'column_names': [None]
            }
        )
        print(ph2)

    def test_value(self):
        sec = [Instrument("AAPL"), Instrument("MSFT")]
        index = pd.MultiIndex.from_product([[date(2021, 1, 1), date(2021, 1, 2)], sec], names=["date", "instruments"])

        prices = History(
            HistorySchema(
                index={"instrument": Instrument, "date": date},
                columns={"open": Number, "close": Number},
            ),
            pd.DataFrame(
                {"close": [100, 200, 105, 205], "open": [99, 199, 104, 204]},
                index=index,
            )
        )

        ph = PortfolioHistory(
            schema_index={"instrument": Instrument, "date": date},
            data=pd.DataFrame({"quantity": [1, 2, 2, 1]}, index=index),
        )

        print(ph.value(prices, "close"))

    def test_insert(self):
        sec = [Instrument("AAPL"), Instrument("MSFT")]
        index = pd.MultiIndex.from_product([[date(2021, 1, 1), date(2021, 1, 2)], sec], names=["date", "instruments"])

        ph = PortfolioHistory(
            schema_index={"instruments": Instrument, "date": date},
            data=pd.DataFrame({"quantity": [1, 2, 2, 1]}, index=index),
        )

        ph.insert(Portfolio({sec[1]: 2}), (date(2021, 1, 3),))
        print(ph)
