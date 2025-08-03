from typing import List, Optional, Callable, Tuple

import pandas as pd

from dxlib.history import History, HistoryView, HistorySchema


class SecuritySignalView(HistoryView):
    time_index: str
    columns: List[str]
    
    def __init__(self, time_index: str = "date", columns: Optional[List[str]] = None):
        self.time_index = time_index
        self.columns = columns if columns else []
        self._apply = self._apply_col if columns is None else self._apply_col

    def len(self, history: History):
        indices = history.index(name=self.time_index)
        return len(indices.unique())

    @classmethod
    def _apply_simple(cls, history: History, function: Callable, output_schema: Optional[HistorySchema]):
        return history.apply({"instrument": function}, output_schema=output_schema)

    def _apply_col(self, history: History, function: Callable, output_schema: Optional[HistorySchema] = None):
        return self._apply_simple(history.get(columns=self.columns), function, output_schema)

    def apply(self, history: History, function: Callable, output_schema: Optional[HistorySchema] = None):
        return self._apply_col(history, function, output_schema)

    def get(self, origin: History, idx):
        return origin.get({self.time_index: [idx]})

    def iter(self, origin: History):
        for idx in origin.index(name=self.time_index):
            yield self.get(origin, idx)

    def price(self, observation: History) -> Tuple[pd.Series, pd.MultiIndex]:
        idx = observation.index(name=self.time_index).unique()
        return observation.data.reset_index(self.time_index)["close"].rename('price'), idx

    def history_schema(self, history_schema: HistorySchema):
        schema = history_schema.copy()
        schema.columns = {key: schema.columns[key] for key in self.columns} if self.columns else schema.columns
        return schema
