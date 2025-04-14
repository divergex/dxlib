import datetime

from dxlib import Executor, History, Cache
import dxlib.execution.signal as ss

from dxlib.interfaces.external.yfinance.yfinance import YFinance


def main():
    market_api = YFinance()
    cache = Cache(".dx")
    storage = "market_data"

    symbols = ["AAPL", "MSFT", "PETR4.SA", "BBAS3.SA"]
    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2024, 12, 31)

    # print interval in years (not date), rounded up
    interval = (end - start).days / 365
    print(f"Interval: {interval:.2f} years")

    history = cache.cached(storage, History, market_api.historical, symbols, start, end)

    print(history.head())

    strategy = ss.SignalStrategy(ss.custom.Rsi())
    executor = Executor(strategy)
    print("Executor")
    print(executor.run(history, ss.views.SecuritySignalView))

if __name__ == "__main__":
    main()
