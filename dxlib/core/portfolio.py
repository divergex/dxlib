import math
import os
from numbers import Number
from typing import Dict, Type, List, Tuple

import pandas as pd

from . import Security
from dxlib.history import History, HistorySchema


def to_tick(x, step):
    return math.floor(x / step) * step


class Portfolio:
    def __init__(self, quantities: Dict[Security, float] = None):
        self.quantities = pd.Series(quantities, name="quantity") if quantities else pd.Series()

    def value(self, prices: pd.Series | Dict[Security, float]) -> float:
        if isinstance(prices, pd.Series):
            return sum(prices * self.quantities)
        else:
            return sum([prices[security] * self.quantities[security] for security in self.securities])

    def weight(self, prices: pd.Series | Dict[Security, float]) -> "Portfolio":
        value = self.value(prices)
        return Portfolio(self.quantities.copy() / value)

    def to_dict(self) -> Dict[Security, float]:
        return self.quantities.to_dict()

    def to_frame(self, column_name="quantity", index_name="security"):
        data = self.quantities.to_frame(column_name)
        data.index.name = index_name
        return data

    def __str__(self):
        return str(self.quantities)

    def update(self, other: "Portfolio"):
        self.quantities = self.quantities.add(other.quantities, fill_value=0)
        return self

    def add(self, security: Security, quantity: float) -> "Portfolio":
        self.quantities[security] = self.quantities.get(security, 0) + quantity
        return self

    def drop_zero(self):
        self.quantities = self.quantities[self.quantities != 0]

    @property
    def securities(self) -> List[Security]:
        return list(self.quantities.keys())


class PortfolioHistory(History):
    """
    A portfolio is a term used to describe a collection of instruments held by an individual or institution.
    Such instruments include but are not limited to stocks, bonds, commodities, and cash.

    A portfolio in the context of this library is a collection of positions, that is, the number of each investment security held.
    """
    def __init__(self,
                 schema_index: Dict[str, Type] = None,
                 data: pd.DataFrame | dict = None):
        assert "security" in list(schema_index.keys()), "Index can not be converted to portfolio type. Must be security indexed at some level."
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

    def apply(self, func: Dict[str | List[str], callable] | callable, *args, **kwargs) -> "PortfolioHistory":
        return self.from_history(
            super().apply(func, *args, **kwargs)
        )

    def value(self, prices: pd.DataFrame, price_column: str) -> History:
        values = self.data["quantity"] * prices[price_column]
        schema = self.history_schema.copy().rename(columns={"quantity": "value"}).set(columns={"value": Number})
        values = History(schema, values.to_frame(name="value"))

        return values.apply({tuple(set(schema.index_names) - {"security"}): lambda x: x.sum()})

    def insert(self, key: pd.MultiIndex, portfolio: "Portfolio"):
        df = portfolio.to_frame()
        if not df.empty:
            key = key.droplevel("security").unique().item()
            portfolio = pd.concat({key: df}, names=list(set(self.history_schema.index_names) - {"security"}))
            self.data = pd.concat([self.data, portfolio])

    def update(self, key: pd.MultiIndex, portfolio: "Portfolio"):
        df = portfolio.to_frame()
        if df.empty:
            return

        key = key.droplevel("security").unique().item()
        index_names = list(set(self.history_schema.index_names) - {"security"})
        new_data = pd.concat({key: df}, names=index_names)

        if not self.data.empty:
            to_drop = self.data.index.droplevel("security") == key
            self.data = self.data.loc[~to_drop]

        self.data = pd.concat([self.data, new_data])