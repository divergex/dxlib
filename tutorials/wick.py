import datetime

from dxlib import Executor, History, Portfolio, Security
from dxlib.interfaces import BacktestInterface
from dxlib.interfaces.external.yfinance import YFinance
from dxlib.strategy.signal.custom.wick_reversal import WickReversal
from dxlib.strategy.signal.order_generator import OrderGenerator
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
    history_view = SecuritySignalView()

    strat = SignalStrategy(WickReversal(), OrderGenerator())
    portfolio = Portfolio({Security("USD"): 1000})
    interface = BacktestInterface(history, portfolio, history_view)
    executor = Executor(strat, interface)
    orders, portfolio = executor.run(history_view)
    print(orders)
    print(portfolio)

if __name__ == "__main__":
    main()
