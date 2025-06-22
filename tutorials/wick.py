import datetime

from dxlib import Executor, History
from dxlib.interfaces.external.yfinance import YFinance
from dxlib.strategy.signal.custom.wick_reversal import WickReversal
from dxlib.strategy.views import SecuritySignalView
from dxlib.strategy.signal import SignalStrategy
from dxlib.data import Storage


def main():
    api = YFinance()
    api.start()

    symbols = ["AAPL", "MSFT", "PETR4.SA"]
    end = datetime.datetime(2025, 3, 1)
    start = datetime.datetime(2025, 1, 1)
    storage = Storage()
    store = "yfinance"

    history = storage.cached(store, api.historical, History, symbols, start, end)

    strat = SignalStrategy(WickReversal())
    executor = Executor(strat)
    res = executor.run(history, SecuritySignalView())
    print(res)

if __name__ == "__main__":
    main()
