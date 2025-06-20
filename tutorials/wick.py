import datetime

from dxlib import Executor
from dxlib.interfaces.external.yfinance import YFinance
from dxlib.strategy.signal import SignalStrategy
from dxlib.strategy.signal.custom.wick_reversal import WickReversal
from dxlib.strategy.views import SecuritySignalView


def main():

    strat = SignalStrategy(WickReversal(), SecuritySignalView())
    executor = Executor(strat)

    api = YFinance()
    api.start()

    symbols = ["AAPL", "MSFT", "PETR4.SA"]
    end = datetime.datetime(2025, 1, 1)
    start = datetime.datetime(2020, 1, 1)
    history = api.historical(symbols, start, end)

    print(history)

if __name__ == "__main__":
    main()