from datetime import datetime

import pandas as pd

import dxlib as dx

cache = dx.storage.Cache()
api = dx.interfaces.InvestingCom()

if not cache.exists("my_portfolio", "stocks", dx.History):
    stocks = api.market_interface.history(["AAPL"], datetime(2021, 10, 1), datetime(2024, 10, 15))
    stocks.store(*cache.load("my_portfolio", "stocks"))
else:
    stocks = dx.History.load(*cache.load("my_portfolio", "stocks"))


portfolio = dx.Portfolio.load(*cache.load("my_portfolio", "portfolio"))

# create sample portfolio
portfolio.data = pd.DataFrame(data={"equity_portfolio": [1.0, 1.5]},
                              index=pd.MultiIndex.from_tuples([(pd.Timestamp("2021-10-04"), "AAPL"), (pd.Timestamp("2021-10-05"), "AAPL")],
                                                              names=["date", "security"]),
                              columns=["equity_portfolio"])

# make row index same as stocks (~700 entries)
portfolio.data = portfolio.data.reindex(stocks.data.index)
portfolio.data = portfolio.data.ffill()

result = portfolio.value(stocks, ["equity_portfolio"]).data
print(result)
