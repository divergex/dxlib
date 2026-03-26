from abc import ABC

from dxlib import InstrumentStore


class MockInstrument(ABC):
    tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "FB"]

    @classmethod
    def store(cls):
        return InstrumentStore.from_symbols(cls.tickers)

    @classmethod
    def instruments(cls):
        return cls.store().instruments
