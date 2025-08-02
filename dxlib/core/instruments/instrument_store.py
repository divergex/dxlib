import uuid
from typing import Dict, List

from dxlib.types import TypeRegistry
from .instrument import Instrument


class InstrumentStore(TypeRegistry):
    def __init__(self, instruments: Dict[str, Instrument] = None):
        super().__init__()
        self.instruments: Dict[str, Instrument] = instruments or {}

    def get(self, symbol: str, default=None):
        return self.instruments.get(symbol, default)

    def __getitem__(self, symbol):
        return self.instruments.get(symbol)

    def __len__(self):
        return len(self.instruments)

    def items(self):
        return self.instruments.items()

    def keys(self):
        return self.instruments.keys()

    def values(self):
        return list(self.instruments.values())

    def list(self, symbols: List[str]) -> List[Instrument]:
        return [self.get(symbol) for symbol in symbols]

    def setdefault(self, symbol: str | Instrument, default: Instrument):
        if isinstance(symbol, str):
            return self.instruments.setdefault(symbol, default)
        return symbol

    def add(self, symbol: str, instrument: Instrument = None):
        self.instruments[symbol] = instrument or Instrument(symbol)

    def extend(self, instruments: Dict[str, Instrument]):
        self.instruments.update(instruments)

    def add_symbols(self, symbols: List[str]):
        for symbol in symbols:
            self.add(symbol)

    def add_instruments(self, instruments: List[Instrument]):
        for instrument in instruments:
            self.instruments[instrument.symbol] = instrument

    @classmethod
    def from_symbols(cls, symbols: List[str]):
        store = InstrumentStore()
        store.add_symbols(symbols)
        return store

    @classmethod
    def from_list(cls, instruments: List[Instrument]):
        store = InstrumentStore()
        store.add_instruments(instruments)
        return store

    def to_list(self) -> List[Instrument]:
        return list(self.instruments.values())

    def __hash__(self):
        return hash(self.instruments)
