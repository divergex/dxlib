import datetime
from functools import reduce

from dxlib import Executor, History
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

    simulator = MidpriceGBM(mean=0, std=1)

    strategy = AvellanedaStoikov()
    view = SecurityPriceView()

    def run_backtest():
        history = History()
        for quotes in simulator.run(10):
            history.concat(quotes)

        interface = BacktestInterface(history, view)
        executor = Executor(strategy, interface, PortfolioContext.bind(interface))
        orders, portfolio_history = executor.run(view, interface.iter())
        value = portfolio_history.value(interface.market.price_history.data)
        return value.data
    
    data = run_backtest()
    print(data)
    print("Value", data.iloc[-1].item())

if __name__ == "__main__":
    main()
