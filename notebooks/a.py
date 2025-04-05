import datetime

from dxlib.interfaces.external.yfinance.yfinance import YFinance
from dxlib.storage import Cache

market_api = YFinance()
cache = Cache(".dx")
storage = "market_data"

symbols = ["AAPL", "MSFT", "PETR4.SA", "BBAS3.SA"]
start = datetime.datetime(2021, 1, 1)
end = datetime.datetime(2024, 12, 31)
data = cache.cached(storage, market_api.historical, symbols, start, end)

print(data.head())
