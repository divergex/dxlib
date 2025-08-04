import math

import numpy as np

from dxlib import Executor, History, Instrument
from dxlib.interfaces import BacktestInterface
from dxlib.interfaces.mock import exponential_decay
from dxlib.market.simulators.fill_model import PoissonLimitFillModel, FillModelRegistry
from dxlib.market.simulators.gbm import MidpriceGBM

from dxlib.strategy import PortfolioContext
from dxlib.strategy.views import SecurityPriceView
from dxlib.strategy.market_making import AvellanedaStoikov


def main():
    simulator = MidpriceGBM(assets=[Instrument("AAPL")], midprice=[1.0], mean=0, vol=.15)

    fill_model = PoissonLimitFillModel(1, exponential_decay(base_rate=0.1, decay_rate=4))
    fill_registry = FillModelRegistry(fill_model)

    strategy = AvellanedaStoikov(gamma=0.1)
    view = SecurityPriceView()

    periods = 252

    def run_backtest():
        history = History()
        for quotes in simulator.run(dt=1/periods, T=1):
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
    df = value.data
    returns = df.pct_change().dropna()
    rf_annual = 0.05
    rf_daily = (1 + rf_annual) ** (1 / periods) - 1
    excess = returns - rf_daily

    x = excess.values.flatten()
    x = x - np.mean(x)
    n = len(x)
    m = math.floor(4 * (n / 100) ** (2/9))
    acov_full = np.correlate(x, x, mode='full') / n

    lags = np.arange(0, m + 1)
    acov = acov_full[n - 1: n - 1 + m + 1]  # gamma_0 ... gamma_m
    weights = 1 - lags / (m + 1)
    long_run_var = acov[0] + 2 * np.sum(weights[1:] * acov[1:])

    annualizer = np.sqrt(252)

    sharpe_naive = excess.mean() / excess.std(ddof=1) * annualizer
    sharpe_adj = excess.mean() / np.sqrt(long_run_var) * annualizer

    print(sharpe_naive.value, sharpe_adj.value)

    import matplotlib.pyplot as plt
    plt.plot(value.data)
    plt.show()

if __name__ == "__main__":
    main()
