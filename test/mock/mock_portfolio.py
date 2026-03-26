from abc import ABC

import pandas as pd

from dxlib import Portfolio, Instrument


class MockPortfolio(ABC):
    tickers = ["AAPL", "MSFT", "GOOG"]

    @classmethod
    def small_portfolio(cls):
        instruments = [Instrument(ticker) for ticker in cls.tickers]

        portfolio = Portfolio({
            instruments[0]: 10,
            instruments[1]: 10,
            instruments[2]: 10,
        })
        return portfolio

    @classmethod
    def small_quantities(cls):
        quantities = pd.Series({"weights": 1, "weights2": 2})
        return quantities

    @classmethod
    def small_time_quantities(cls):
        quantities = pd.DataFrame(data={"weights1": [1.0], "weights2": [None, 2.0]},
                                  index=pd.MultiIndex.from_tuples([("2021-01-01", "AAPL"), ("2021-01-02", "AAPL")],
                                                                  names=["date", "instrument"]),
                                  columns=["inventory1", "inventory2"])
        return quantities
