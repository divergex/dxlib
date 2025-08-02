import datetime
from functools import reduce

from dxlib import Executor
from dxlib.interfaces import BacktestInterface
from dxlib.market.simulators.gbm import MidpriceGBM

from dxlib.strategy import PortfolioContext
from dxlib.strategy.views import SecurityPriceView
from dxlib.strategy.market_making import AvellanedaStoikov


def main():
    symbols = ["AAPL", "MSFT", "PETR4.SA", "BBAS3.SA"]
    end = datetime.datetime(2025, 7, 29)
    start = end - datetime.timedelta(hours=24)

    interval = (end - start).days / 365
    print(f"Interval: {interval:.2f} years")

    quotes = MidpriceGBM()
    history = reduce(lambda a, b: a.concat(b), quotes.run(10))

    print(history.head())

    strategy = AvellanedaStoikov()
    interface = BacktestInterface(history, view := SecurityPriceView())
    executor = Executor(strategy, interface, PortfolioContext.bind(interface))
    print(executor.run(view, view.iter(history)))

if __name__ == "__main__":
    main()
