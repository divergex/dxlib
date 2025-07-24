from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from dxlib import Instrument, History, Portfolio, InstrumentStore
from dxlib.data import Storage
from dxlib.interfaces import MarketInterface, yfinance
from dxlib.strategy.optimizers.mvo import Mvo
from dxlib.strategy.signal import OrderGenerator


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
            storage.cached(key, get_instrument, Instrument, symbol)
            for symbol in symbols
        ],
        key)
    assets = asset_store.values()

    end = datetime(2025, 7, 3)
    start = end - timedelta(days=360)
    history = storage.cached(key, api.historical, History, assets, start, end, asset_store)

    trading_days = 252
    returns = history.get(columns=["close"]).apply({"instruments": lambda x: np.log(x / x.shift(1))})
    expected_returns: pd.DataFrame = returns.apply({"instruments": lambda x: x.mean() * trading_days / 12}).data
    covariance_returns = returns.op(lambda x: x.unstack("instruments").cov())

    optim = Mvo()
    print("Inputs:")
    print(expected_returns)
    print(covariance_returns)
    w, _, _ = optim(expected_returns.to_numpy(), covariance_returns.to_numpy(), gamma := 5e-2)
    prices = history.get(index={"date": [returns.index("date")[0]]}).data.reset_index("date")["close"]
    current = Portfolio.from_weights({sec: 1 / len(assets) for sec in assets}, prices)
    target = Portfolio({
        expected_returns.index[i]: w[i] for i in range(len(expected_returns))
    })
    orders = OrderGenerator()
    print("Current:")
    print(current)
    print("Orders to target:")
    print(orders.from_target(current, target))


if __name__ == "__main__":
    main()
