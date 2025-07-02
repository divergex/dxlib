from datetime import datetime
from typing import Iterator, List

import pandas as pd

from dxlib.core import Security
from dxlib.history import History, HistorySchema, HistoryView
from .interface import Interface


class MarketInterface(Interface):
    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def quote(self, symbols: List[str] | str | Security | List[Security]) -> float | pd.DataFrame:
        """
        Get the current price of the security.
        """
        raise NotImplementedError

    def subscribe(self, history_view: HistoryView) -> Iterator:
        """
        Listen to updates. Forms um `historical`.
        """
        raise NotImplementedError

    def historical(self, symbols: list[str], start: datetime, end: datetime, interval: str) -> History:
        """
        Get the historical price of the security.
        """
        raise NotImplementedError

    def history_schema(self) -> HistorySchema:
        """
        Return the schema of the historical and subscribe data.
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not implement history_schema")
