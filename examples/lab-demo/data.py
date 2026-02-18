import datetime

import dxlib as dx
import test
from dxlib.data.storage.cache import Cache
from dxlib.interfaces.external import yfinance


def get_data():
    # TODO: #new-feature market screener instead of manually providing symbols
    instruments = dx.InstrumentStore.from_symbols(["AAPL", "MSFT", "PETR4.SA", "BBAS3.SA"])

    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2024, 12, 31)

    api: dx.interfaces.MarketInterface = yfinance.YFinance()
    api.start()

    history = api.historical(instruments.symbols(), start, end, "1d")

    return history

def test_data():
    history = test.data.Mock.large_history()
    return history


def cached_data():
    # lets cache it
    storage = Cache()
    namespace = "data"

    history = storage.cache(namespace, dx.History, test_data)

    return history
