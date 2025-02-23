import os
from typing import Dict, List, Type
from functools import reduce

import pandas as pd

from dxlib.history import History, HistorySchema


class Portfolio(History):
    """
    A portfolio is a term used to describe a collection of investments held by an individual or institution.
    Such investments include but are not limited to stocks, bonds, commodities, and cash.

    A portfolio in the context of this library is a collection of positions, that is, the number of each security held.
    """
    def __init__(self,
                 index: Dict[str, Type] = None,
                 inventories: List[str] = None,
                 data: pd.DataFrame = None):

        """
        A portfolio is a collection of investments held by an individual or institution.

        A portfolio in the context of this library is a collection of positions, that is, the number of each security held.

        Args:
            index (Dict[str, Type]): The index of the portfolio. Can be used elsewhere.
            inventories (List[str]): The inventories of the portfolio. Can be used elsewhere.
            data (Dict[str, Dict[str, float]]): The data for which this portfolio is a container. Can be used elsewhere.

        Returns:
            Portfolio: A portfolio instance.
        """
        schema = HistorySchema(index=index, columns={inventory: float for inventory in inventories or []})
        super().__init__(schema, data)

    def value(self,
              prices: History,
              inventories: List['str'] = None,
              aggregate_inventories: bool = True,
              ) -> History:
        """
        For each column of `other`, evaluate the listed values for matching indices in each Portfolio's inventory,
        or for specific selected inventories only.
        """
        result = {
            inventory: prices.data.mul(self.data[inventory], axis=0)
            for inventory in (self.columns if inventories is None else inventories)
        }

        if aggregate_inventories:
            result = reduce(lambda a, b: a.add(b, fill_value=0), result.values())
            return History(self.schema, result)

        elif len(result) == 1:
            result = list(result.values())[0:]

        return History(self.schema, pd.concat(result))

    def weights(self,
                prices: History,
                securities="security"):
        value = self.value(prices).data.dropna()
        data = value.sum(axis=1).unstack(securities)
        aggregated = data.sum(axis=1)

        result = data.divide(aggregated, axis=0)

        return History(self.schema, result.stack(securities).to_frame(name="weight"))

    def agg(self, securities="security", cumsum=True, aggregate_inventories: bool = True):
        data = self.data
        inventories = self.columns.copy()

        if cumsum:
            data = data.groupby(securities, group_keys=False).cumsum()

        if aggregate_inventories:
            data = data.sum(axis=1).to_frame("inventory")
            inventories = ["inventory"]

        return Portfolio(self.schema.index.copy(), inventories, data)

    def store(self, storage_path: str, key: str):
        try:
            os.makedirs(storage_path + "/portfolio")
        except FileExistsError:
            pass

        history_storage = storage_path + "/portfolio"
        self.data.to_hdf(history_storage + f"/data.h5", key=key, mode='w', format='f')
        self.schema.store(history_storage + f"/schema", key)

    @classmethod
    def load(cls, storage_path, key):
        history_storage = storage_path + "/portfolio"
        data = pd.read_hdf(history_storage + "/data.h5", key, mode='r')
        schema = HistorySchema.load(history_storage + "/schema", key)
        assert isinstance(data, pd.DataFrame)
        return cls(schema.index, schema.columns, data)

    @classmethod
    def cache_exists(cls, cache_path, key):
        history_storage = cache_path + "/portfolio"
        return os.path.exists(history_storage + f"/data.h5") and os.path.exists(history_storage + f"/schema")