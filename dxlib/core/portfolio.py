import math
from numbers import Number
from typing import Dict, Type, List, Union, Optional, Callable

import pandas as pd
from pandas import Series

from .instruments import Instrument
from dxlib.history import History, HistorySchema


def to_tick(x, step):
    return math.floor(x / step) * step


class Portfolio:
    quantities: Series

    def __init__(self, quantities: Optional[Dict[Instrument, float]] = None):
        self.quantities: pd.Series = pd.Series(quantities, dtype=float) if quantities else pd.Series()

    def value(self, prices: pd.Series | Dict[Instrument, float]) -> float:
        if isinstance(prices, pd.Series):
            return sum(prices * self.quantities)
        else:
            return sum([prices[security] * self.quantities[security] for security in self.securities])

    def weight(self, prices: pd.Series | Dict[Instrument, float]) -> "Portfolio":
        value = self.value(prices)
        return Portfolio(self.quantities.copy() / value)

    @staticmethod
    def ensure_index(indexer, to_reindex):
        if not indexer.index.equals(to_reindex.index):
            to_reindex = to_reindex.reindex(indexer.index)
            isnull = to_reindex.isnull()
            if isnull.any():
                raise ValueError(f"Missing prices for: {to_reindex[isnull].index.tolist()}")

    @classmethod
    def from_weights(cls,
                     weights: Union[pd.Series, Dict["Instrument", float]],
                     prices: Union[pd.Series, Dict["Instrument", float]],
                     value: float
                     ) -> "Portfolio":
        """

        Args:
            prices:
            weights:
            value (float): Total value of the portfolio.
        """
        weights = pd.Series(weights)
        prices = pd.Series(prices)

        cls.ensure_index(weights, prices)

        quantities = weights * value / prices

        return cls(quantities.to_dict())

    @classmethod
    def from_values(cls,
                    values: Union[pd.Series, Dict["Instrument", float]],
                    prices: Union[pd.Series, Dict["Instrument", float]],
                    ):
        # transform values / prices -> quantities
        values = pd.Series(values)
        prices = pd.Series(prices)
        total = values.sum()
        return cls.from_weights(values / total, prices, total)

    @classmethod
    def from_series(cls,
                    quantities: pd.Series,
                    ):
        return cls(quantities.to_dict())

    def get(self, security: Instrument, default: float = 0.0) -> float:
        val = self.quantities.get(security, default)
        assert isinstance(val, float)
        return val

    def to_dict(self) -> Dict[Instrument, float]:
        return self.quantities.to_dict()

    def to_frame(self, column_name="quantity", index_name="instrument"):
        data = self.quantities.to_frame(column_name)
        data.index.name = index_name
        return data

    def __str__(self):
        return str(self.quantities)

    def update(self, other: "Portfolio"):
        self.quantities = self.quantities.add(other.quantities, fill_value=0)
        return self

    def add(self, security: Instrument, quantity: float) -> "Portfolio":
        self.quantities[security] = self.get(security) + quantity
        return self

    def drop_zero(self):
        self.quantities = self.quantities.loc[self.quantities != 0]

    @property
    def securities(self) -> List[Instrument]:
        return list(self.quantities.keys())


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

        self.data = pd.concat([self.data, new_data])
