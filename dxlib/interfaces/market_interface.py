from datetime import datetime
from typing import Iterator, List, Optional

import pandas as pd

from dxlib.core import Instrument, InstrumentStore
from dxlib.history import History, HistorySchema, HistoryView
from .interface import Interface


class MarketInterface(Interface):
    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def quote(self, symbols: List[str] | str | Instrument | List[Instrument]) -> pd.Series:
        """
        Get the current price of the instruments.
        """
        raise NotImplementedError

    def subscribe(self, history_view: HistoryView) -> Iterator:
        """
        Listen to updates. Forms um `historical`.
        """
        raise NotImplementedError

    def historical(self, symbols: list[str], start: datetime, end: datetime, interval: str, store: Optional[InstrumentStore] = None) -> History:
        """
        Get the historical price of the instruments.
        """
        raise NotImplementedError

    def history_schema(self) -> HistorySchema:
        """
        Return the schema of the historical and subscribe data.
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not implement history_schema")

    def symbols(self, query: str) -> List[str]:
        raise NotImplementedError(f"{self.__class__.__name__} does not implement symbols")