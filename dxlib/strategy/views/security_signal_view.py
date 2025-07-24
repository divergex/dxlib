from typing import List

import pandas as pd

from dxlib import History
from dxlib.history.history_view import HistoryView


class SecuritySignalView(HistoryView):
    def __init__(self, columns: List[str] = None):
        self.columns = columns
        self._apply = self._apply_col if columns is None else self._apply_col

    def len(self, history: History):
        indices = history.index(name="date")
        return len(indices.unique())

    @classmethod
    def _apply_simple(cls, history: History, function: callable):
        return history.apply({"instruments": function})

    def _apply_col(self, history: History, function: callable):
        return self._apply_simple(history.get(columns=self.columns), function)

    def apply(self, history: History, function: callable):
        return self._apply_col(history, function)

    def get(self, origin: History, idx):
        return origin.get({"date": [idx]})

    def iter(self, origin: History):
        for idx in origin.index(name="date"):
            yield self.get(origin, idx)

    def price(self, origin: History, idx: pd.MultiIndex) -> pd.Series:
        date = idx.get_level_values("date").unique().item()
        dated_prices = origin.get({"date": [date]})
        return dated_prices.data.reset_index("date")["close"].rename('price')
