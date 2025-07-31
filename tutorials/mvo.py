from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from dxlib import Instrument, History, Portfolio, InstrumentStore
from dxlib.data import Storage
from dxlib.interfaces import MarketInterface, yfinance
from dxlib.optimizers.mvo import Mvo
from dxlib.strategy import OrderGenerator


def main():
    api: MarketInterface = yfinance.YFinance()
    api.start()

    def get_instrument(query):
        return Instrument(api.symbols(query)[0])

    storage = Storage()
    key = "yfinance"

    symbols = ["TOTV", "PETR4", "MGLU3", "VALE3"]
    asset_store = InstrumentStore.from_list(
        [
            storage.cached(key, Instrument, get_instrument, symbol)
            for symbol in symbols
        ])
    assets = asset_store.values()

    end = datetime(2025, 7, 3)
    start = end - timedelta(days=360)
    history = storage.cached(key, History, api.historical, assets, start, end, asset_store)

    trading_days = 252
    returns = history.get(columns=["close"]).apply({"instruments": lambda x: np.log(x / x.shift(1))})
    expected_returns: pd.Series = returns.apply({"instruments": lambda x: x.mean() * trading_days / 12}).data
    covariance_returns = returns.op(lambda x: x.unstack("instruments").cov())

    prices = history.get(index={"date": [returns.index("date")[0]]}).data.reset_index("date")["close"]
    current = Portfolio.from_weights({sec: 1 / len(assets) for sec in assets}, prices, total_value := 1_000)
    orders = OrderGenerator()
    print("Current:")
    print(current)

    optimizer = Mvo()
    print("Expected returns:")
    print(expected_returns)
    print("Covariance:")
    print(covariance_returns)
    weights, _ = optimizer.optimize(expected_returns, covariance_returns.to_numpy(), gamma := 5e-2)
    target = Portfolio.from_series(weights * total_value / prices)

    print("Target:")
    print(target)

    print("Orders to target:")
    print(orders.from_target(current, target))

if __name__ == "__main__":
    main()
