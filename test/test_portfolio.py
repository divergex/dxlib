import unittest

import pandas as pd

import dxlib as dx
from dxlib import History
from utils import Mock


class TestPortfolio(unittest.TestCase):
    def test_portfolio(self):
        schema = Mock().schema

        quantitites = pd.DataFrame(data={"weights1": [1.0], "weights2": [None, 2.0]},
            index=pd.MultiIndex.from_tuples([("2021-01-01", "AAPL"), ("2021-01-02", "AAPL")], names=["date", "instruments"]),
            columns=["inventory1", "inventory2"])

        portfolio = dx.Portfolio(index=schema.index, inventories=["inventory1"], data=quantitites)
        print(portfolio)

        prices = History(Mock().schema, Mock().tight_data)
        values = portfolio.value(prices)

        print(values.data.dropna())

    def test_weights(self):
        schema = Mock().schema

        quantitites = pd.DataFrame(data={"inventory1": [1.0, 1.5], "inventory2": [3.2, 2.0]},
            index=pd.MultiIndex.from_tuples([("2021-01-01", "AAPL"), ("2021-01-02", "AAPL")], names=["date", "instruments"]),
            columns=["inventory1", "inventory2"])

        portfolio = dx.Portfolio(index=schema.index, inventories=["inventory1"], data=quantitites)
        print(portfolio)

        prices = History(Mock().schema, Mock().tight_data)
        weights = portfolio.weights(prices, "instruments")

        returns = prices.apply({"instruments": lambda df: df.pct_change()})
        portfolio_returns = weights.apply_on(returns.data['open'], lambda df, other: df.mul(other, axis=0)).data
        portfolio_returns.columns = ["returns"]

        print(portfolio_returns.dropna() * 100)

    def test_cumsum(self):
        schema = Mock().schema

        quantitites = pd.DataFrame(data={"inventory1": [1.0, 1.5], "inventory2": [3.2, 2.0]},
                                   index=pd.MultiIndex.from_tuples([("2021-01-01", "AAPL"), ("2021-01-02", "AAPL")],
                                                                   names=["date", "instruments"]),
                                   columns=["inventory1", "inventory2"])

        portfolio = dx.Portfolio(index=schema.index, inventories=["inventory1"], data=quantitites)
        print(portfolio)

        print(portfolio.agg())

if __name__ == '__main__':
    unittest.main()
