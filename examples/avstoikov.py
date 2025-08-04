from dxlib import Executor, History, Instrument
from dxlib.interfaces import BacktestInterface
from dxlib.interfaces.mock import exponential_decay
from dxlib.interfaces.mock.fill_model import PoissonLimitFillModel, FillModelRegistry
from dxlib.market.simulators.gbm import MidpriceGBM

from dxlib.strategy import PortfolioContext
from dxlib.strategy.views import SecurityPriceView
from dxlib.strategy.market_making import AvellanedaStoikov


def main():
    simulator = MidpriceGBM(assets=[Instrument("AAPL")], mean=0, vol=.01)

    fill_model = PoissonLimitFillModel(1, exponential_decay(0.07, 1.5))
    fill_registry = FillModelRegistry(fill_model)

    strategy = AvellanedaStoikov(gamma=0.1)
    view = SecurityPriceView()

    def run_backtest():
        history = History()
        for quotes in simulator.run(10):
            history.concat(quotes)

        interface = BacktestInterface(history, view, fill_registry=fill_registry)
        executor = Executor(strategy, interface, PortfolioContext.bind(interface))
        orders, portfolio_history = executor.run(view, interface.iter())
        value = portfolio_history.value(interface.market.price_history.data)
        return history, portfolio_history, value
    
    history, portfolio_history, value = run_backtest()
    print(history.data)
    print(portfolio_history.data)
    print(value.data)

if __name__ == "__main__":
    main()
