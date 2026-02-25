import math
from typing import Dict, List, Union, Optional

import pandas as pd
from pandas import Series

from dxlib.data.storage import StoredField, FieldFormat, Storable
from ..instruments import Instrument


def to_tick(x, step):
    return math.floor(x / step) * step


class Portfolio(Storable):
    """
    A portfolio is a term used to describe a collection of instruments held by an individual or institution.
    """
    quantities: Series = StoredField(FieldFormat.DATAFRAME, Series)

    def __init__(self,
                 quantities: Optional[Union[Dict[Instrument, float], pd.Series]] = None
                 ):
        self.quantities: pd.Series = pd.Series(quantities, dtype=float) if quantities else pd.Series()

    def value(self,
              prices: Union[pd.Series, Dict[Instrument, float]]
              ) -> float:
        if isinstance(prices, pd.Series):
            return sum(prices * self.quantities)
        else:
            return sum([prices[security] * self.quantities[security] for security in self.securities])

    def weight(self,
               prices: Union[pd.Series | Dict[Instrument, float]]
               ) -> "Portfolio":
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
