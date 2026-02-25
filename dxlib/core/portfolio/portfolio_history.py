from numbers import Number
from typing import Dict, Type, List, Optional, Callable

import pandas as pd

from dxlib.history import History, HistorySchema


class PortfolioHistory(History):
    """
    A portfolio is a term used to describe a collection of instruments held by an individual or institution.
    Such instruments include but are not limited to stocks, bonds, commodities, and cash.

    A portfolio in the context of this library is a collection of positions, that is, the number of each investment instruments held.
    """
    def __init__(self,
                 schema_index: Dict[str, Type],
                 data: Optional[pd.DataFrame | dict] = None):
        assert "instrument" in list(
            schema_index.keys()), "Index can not be converted to portfolio type. Must have instruments indexed at some level."
        schema = HistorySchema(
            index=schema_index,
            columns={"quantity": Number},
        )
        super().__init__(schema, data)

    @classmethod
    def from_history(cls, history: History) -> "PortfolioHistory":
        return PortfolioHistory(
            schema_index=history.history_schema.index,
            data=history.data,
        )

    def apply(self, func: Dict[str | List[str], Callable] | Callable, *args, **kwargs) -> "PortfolioHistory":
        return self.from_history(
            super().apply(func, *args, **kwargs)
        )

    def value(self, prices: pd.DataFrame, price_column: str = "price") -> History:
        values = (self.data["quantity"] * prices[price_column]).dropna()
        schema = self.history_schema.copy().rename(columns={"quantity": "value"}).set(columns={"value": Number})
        values = History(schema, values.to_frame(name="value"))

        return values.apply({tuple(set(schema.index_names) - {"instrument"}): lambda x: x.sum()})

    def insert(self, key: pd.MultiIndex, portfolio: "Portfolio"):
        df = portfolio.to_frame()
        if not df.empty:
            key = key.droplevel("instrument").unique().item()
            portfolio = pd.concat({key: df}, names=list(set(self.history_schema.index_names) - {"instrument"}))
            self.data = pd.concat([self.data, portfolio])

    def update(self, key: pd.MultiIndex | pd.Index, portfolio: "Portfolio"):
        df = portfolio.to_frame()
        if df.empty:
            return

        key = key.droplevel("instrument").unique().item()
        index_names = list(set(self.history_schema.index_names) - {"instrument"})
        new_data = pd.concat({key: df}, names=index_names)

        if not self.data.empty:
            to_drop = self.data.index.droplevel("instrument") == key
            self.data = self.data.loc[~to_drop]

        self.data = pd.concat([self.data if not self.data.empty else None, new_data])
