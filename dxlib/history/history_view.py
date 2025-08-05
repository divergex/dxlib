from abc import ABC, abstractmethod
from typing import Iterable, Callable, Optional, Any, Tuple

import pandas as pd

from . import History, HistorySchema


class HistoryView(ABC):
    @staticmethod
    @abstractmethod
    def len(history: History):
        pass

    @staticmethod
    @abstractmethod
    def apply(history: History, function: Callable, output_schema: Optional[HistorySchema] = None) -> History:
        pass

    @staticmethod
    @abstractmethod
    def get(origin: History, idx):
        pass

    @staticmethod
    @abstractmethod
    def iter(origin: History) -> Iterable[History]:
        pass

    @staticmethod
    def price(observation: History) -> Tuple[pd.Series, pd.MultiIndex]:
        raise NotImplementedError("Pricing is not implemented. Perhaps you are backtesting with an invalid HistoryView?")

    @staticmethod
    @abstractmethod
    def history_schema(history_schema: HistorySchema) -> HistorySchema:
        pass