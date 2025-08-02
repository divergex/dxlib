import datetime

from dxlib import Executor, History, InstrumentStore, OrderGenerator
from dxlib.data import Storage
from dxlib.strategy import views
from dxlib.strategy import signal as ss

import dxlib.interfaces.external.yfinance as yfinance
from dxlib.interfaces import BacktestInterface


def main():
    api = yfinance.YFinance()

    cache = Storage(".divergex")
    store = "yfinance"

    assets = InstrumentStore.from_symbols(["AAPL", "MSFT", "PETR4.SA", "BBAS3.SA"])
    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2024, 12, 31)

    interval = (end - start).days / 365
    print(f"Interval: {interval:.2f} years")

    if not cache.exists(store, api.historical, assets.to_list(), start, end, "1d", assets):
        api.start()

    history = cache.cached(store, History, api.historical, assets.to_list(), start, end, "1d", assets)

    print(history.head())

    strategy = ss.SignalStrategy(ss.custom.Rsi(), OrderGenerator())
    executor = Executor(strategy, BacktestInterface(history, history_view := views.SecuritySignalView(["close"])))
    print(executor.run(history_view, history))

if __name__ == "__main__":
    main()
