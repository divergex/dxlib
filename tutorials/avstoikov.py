import datetime

from dxlib import Executor, History
from dxlib.data import Storage
from dxlib.interfaces import BacktestInterface

from dxlib.strategy.market_making import AvellanedaStoikov

from dxlib.interfaces.external.yfinance.yfinance import YFinance
from dxlib.strategy.market_making.avstoi_strategy import PortfolioContext
from dxlib.strategy.views.security_quotes_view import SecurityQuotes


def main():
    api = YFinance()
    api.start()
    cache = Storage(".divergex")
    storage = "yfinance"

    symbols = ["AAPL", "MSFT", "PETR4.SA", "BBAS3.SA"]
    end = datetime.datetime(2025, 7, 29)
    start = end - datetime.timedelta(hours=24)

    interval = (end - start).days / 365
    print(f"Interval: {interval:.2f} years")

    history = cache.cached(storage, History, api.quote, symbols, start, end, "1m")

    print(history.head())

    strategy = AvellanedaStoikov()
    interface = BacktestInterface(history, view := SecurityQuotes())
    executor = Executor(strategy, interface, PortfolioContext.bind(interface))
    print(executor.run(view, history))

if __name__ == "__main__":
    main()
