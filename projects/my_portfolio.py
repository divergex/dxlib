from datetime import datetime

import pandas as pd

import dxlib as dx

cache = dx.storage.Cache()
api = dx.interfaces.InvestingCom()

if not cache.exists("my_portfolio", "stocks", dx.History):
    stocks = api.market_interface.history(["AAPL", "PETR4", "NVDC34", "TSM", "AMZO34"], datetime(2021, 10, 1), datetime(2024, 10, 15))
    stocks.store(*cache.load("my_portfolio", "stocks"))
else:
    stocks = dx.History.load(*cache.load("my_portfolio", "stocks"))

if not cache.exists("my_portfolio", "portfolio", dx.Portfolio):
    portfolio = dx.Portfolio(stocks.schema.index, ["equity_portfolio"], pd.DataFrame(
        data={"equity_portfolio": [1.0, 1.5]},
        index=pd.MultiIndex.from_tuples([(pd.Timestamp("2021-10-04"), "AAPL"), (pd.Timestamp("2021-10-05"), "AAPL")],
                                        names=["date", "security"]),
        columns=["equity_portfolio"]
    ))
    portfolio.store(*cache.load("my_portfolio", "portfolio"))
else:
    portfolio = dx.Portfolio.load(*cache.load("my_portfolio", "portfolio"))

price = stocks.get(columns=["close"])
returns = price.apply({"security": lambda df: df.pct_change()})
returns = returns.dropna()

mean_returns = returns.apply({"security": lambda df: df.mean()})
mean_returns = mean_returns.data
cov_matrix = returns.data.reset_index(level="security").pivot(columns="security").dropna().cov()

weights = dx.strategies.mean_variance_optim(mean_returns, cov_matrix)
print(weights)

"""
portfolio.data = portfolio.data.reindex(stocks.data.index)
portfolio.data = portfolio.data.ffill()

result = portfolio.value(stocks, ["equity_portfolio"]).data
print(result)
"""