from datetime import datetime

from dxlib.history import History, HistorySchema
from .interface import Interface


class MarketInterface(Interface):
    def quote(self) -> float:
        """
        Get the current price of the security.
        """
        raise NotImplementedError

    def bar(self) -> float:
        """
        Get the current price of the security.
        """
        raise NotImplementedError

    def historical(self, symbols: list[str], start: datetime, end: datetime, interval: str) -> History:
        """
        Get the historical price of the security.
        """
        raise NotImplementedError

    @property
    def history_schema(self) -> HistorySchema:
        """
        Return the schema of the historical data.
        """
        raise NotImplementedError
