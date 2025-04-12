import os
from functools import reduce
from typing import Dict, List, Union, Tuple

import numpy as np
import pandas as pd

from dxlib.storage import Serializable, RegistryBase
from .history_schema import HistorySchema


class History(Serializable, metaclass=RegistryBase):
    """
    A history is a term used to describe a collection of data points.

    A history in the context of this library is extended to include the concept of a time series.
    Usually the data points are indexed by time and sometimes by a security.

    The main purpose of a history is to provide common methods to manipulate and analyze the data, as well as context.
    This is useful for easily storing, retrieving, backtesting and networking data.
    """

    def __init__(self,
                 history_schema: HistorySchema | dict = None,
                 data: pd.DataFrame | dict | list = None):
        """
        A history is a collection of dense mutable data points.

        It is described by a schema, that can be used to navigate and manipulate the data,
        as well as to provide context.

        The schema is necessary for any operations on data, but is not required for initialization,
        as a history instance can be used as a placeholder for data.

        Args:
            history_schema (HistorySchema): The schema that describes the data.
            data (pd.DataFrame | dict | list): The data for which this history is a container. Can be used elsewhere.

        Returns:
            History: A history instance.
        """
        self.history_schema: HistorySchema = history_schema
        self.data: pd.DataFrame | None = None

        if isinstance(history_schema, dict):
            self.history_schema = HistorySchema.from_dict(history_schema)

        if isinstance(data, pd.DataFrame):
            self.data: pd.DataFrame = data
        elif isinstance(data, dict):
            self.data: pd.DataFrame = pd.DataFrame.from_dict(data, orient="tight")
        elif isinstance(data, list):
            self.data: pd.DataFrame = pd.DataFrame(data)

        # test if self.data index have correct names
        if self.history_schema is not None and self.data is not None:
            if self.history_schema.index and self.history_schema.index.keys() != set(self.data.index.names):
                raise ValueError("The index names do not match the schema.")

    # region Abstract Properties

    def idx(self, name):
        """
        Get the level of the index by name.

        Args:
            name (str): The name of the index level.

        Returns:
            int: The level of the index.
        """
        return self.data.index.names.index(name)

    def iidx(self, idx) -> str:
        """
        Get the name of the index by level.

        Args:
            idx (int): The level of the index.

        Returns:
            str: The name of the index.
        """
        return self.data.index.names[idx]

    def levels(self, names: List[str] | str = None, to_list=True) -> list | Dict[str, list]:
        """
        Get the levels of the index by name.

        Args:
            names (List[str] | str): The names of the index levels.
            to_list (bool): Whether to return a pandas or list object.

        Returns:
            list | Dict[str, list]: The levels of the index.
        """
        if isinstance(names, str):
            obj = self.data.index.get_level_values(self.idx(names)).unique()
            return obj.tolist() if to_list else obj
        else:
            return {name: self.levels(name) for name in (names if names else self.indices)}

    def index(self, name: str) -> pd.Index:
        """
        Get the index by name.

        Args:
            name (str): The name of the index.

        Returns:
            pd.Index: The index.
        """
        return self.data.index.get_level_values(name)

    @property
    def indices(self):
        """
        Get the indices of the history.

        Returns:
            List[str]: The indices of the history.
        """
        return self.data.index.names if self.data is not None else []

    @property
    def columns(self):
        """
        Get the columns of the history.

        Returns:
            List[str]: The columns of the history.
        """
        return self.data.columns.values if self.data is not None else []

    # endregion

    # region Manipulation

    def concat(self, other: "History", keep="first") -> "History":
        """
        Complements current history with another histories content. Ignores repeated data.

        Args:
            other (History): The history to add to this history.
            keep (Literal["first", "last"]): The strategy to use when keeping repeated data.

        Returns:
            History: This history, now with the data of the other history.
        """
        if self.history_schema != other.history_schema:
            raise ValueError("The schemas of the histories do not match.")

        self.data = pd.concat([self.data, other.data])
        self.data = self.data[~self.data.index.duplicated(keep=keep)]
        return self

    def add(self, other):
        if isinstance(other, pd.DataFrame):
            return self.data.add(other)

    def extend(self, other: "History") -> "History":
        """
        Extends current history columns with another histories columns.

        Args:
            other (History): The history to extend this history with.

        Returns:
            History: This history, now the extended column set of the other history.
        """
        if self.history_schema.index != other.history_schema.index:
            raise ValueError("The indexes of the histories do not match.")

        self.history_schema.columns.update(other.history_schema.columns)

        self.data = pd.concat([self.data, other.data], axis=1)
        self.data = self.data.groupby(level=self.data.index.names).first()
        self.data = self.data.T.groupby(self.data.columns).first().T
        return self

    def loc(self, index: List[Union[tuple, str]] = None, columns: List[str] | str = None) -> "History":
        """byu
        Get a subset of the history, given values or a slice of desired index values for each index.

        Args:
            index (Dict[str, slice]): The desired index values for each index.
            columns (List[str] | str): The desired columns.

        Example:
            >>> history = History()
            >>> history.loc(index=[("2021-01-01", "AAPL"), ("2021-01-02", "AAPL")])
            # Returns a history with only the data for the dates 2021-01-01 and 2021-01-02 for AAPL.
        """
        if columns is None:
            columns = self.data.columns
        try:
            available_columns = self.data.columns.intersection(columns)
            if index is not None:
                available_index = self.data.index.intersection(index)
                data = self.data.loc[available_index, available_columns]
            else:
                data = self.data.loc[:, available_columns]
            return History(history_schema=self.history_schema.copy(), data=data)
        except KeyError:
            return History(history_schema=self.history_schema.copy(), data=pd.DataFrame(columns=columns, index=self.data.index))

    def get(self, index: Dict[str, slice | list] = None, columns: List[str] | str = None, raw=False) -> "History":
        """
        Get a subset of the history, given values or a slice of desired index values for each index.

        Args:
            index (Dict[str, slice]): The desired index values for each index.
            columns (List[str] | str): The desired columns.
            raw (bool): If True, returns the raw data.

        Example:
            >>> history = History()
            >>> history.get(index={"date": slice("2021-01-01", "2021-01-03")})
            # Returns a history with only the data for the dates 2021-01-01 to 2021-01-03.
        """
        index = index or {}
        columns = columns or self.columns

        index_filters = [self.data.index.get_level_values(idx).isin(values) for idx, values in index.items() if
                         isinstance(values, list)]
        index_slices = [index.get(level, slice(None)) for level in index.keys() if isinstance(index[level], slice)]

        data = self.data

        if index_filters:
            masks = reduce(lambda x, y: x & y, index_filters)
            data = data[masks]

        if index_slices:
            idx = pd.IndexSlice
            data = data.sort_index().loc[idx[tuple(index_slices), columns]]

        schema = HistorySchema(
            self.history_schema.index.copy(),
            {col: self.history_schema.columns[col] for col in columns}
        )

        return History(schema, data[columns]) if not raw else data

    def set(self, values: Dict | pd.DataFrame):
        """
        Set values in the history data based on index filters and column selection. Doesn' work with slices yet.

        Example:
            >>> history = History()
            >>> history.set({("2021-01-01", "AAPL"): {"close": 100}})
            # Sets the close price of AAPL on 2021-01-01 to 100.

        """
        # don't use for, as too slow
        if isinstance(values, dict):

            values = pd.DataFrame.from_dict(values, orient="tight")
        elif not isinstance(values, pd.DataFrame):
            raise ValueError("Values must be a dictionary or a DataFrame.")

        # update and then concat only new values, so as to not create repeated rows nor ignore existing column values
        self.data.update(values)
        self.data = pd.concat([self.data, values], sort=False).groupby(level=self.data.index.names).first()

    def op(self,
           other: "History",
           columns: List[any],
           other_columns: List[any],
           operation: callable) -> "History":
        """
        Apply a given callable operation on matching indices of self and other,
        for all cartesian product of columns x other_columns.

        WARNING: This is a very slow operation, taking about 1ms per N x M row of self and other.
        """
        a = self.data
        b = other.data

        if isinstance(other_columns, str):
            other_columns = [other_columns]

        if not set(b.index.names).issubset(set(a.index.names)):
            raise ValueError("The self dataframe must have at least all the index names of the other dataframe.")

        common_levels = list(set(b.index.names) & set(a.index.names))

        aligned_self = a[columns].reset_index()
        aligned_other = b[other_columns].reset_index()

        renamed_other = aligned_other.rename(columns={col: f"<other>{col}" for col in other_columns})

        merged_df = pd.merge(aligned_self, renamed_other, on=common_levels)
        self_selected = merged_df[columns].values
        other_selected = merged_df[[f"<other>{col}" for col in other_columns]].values

        combined = operation(self_selected[:, np.newaxis, :], other_selected[:, :, np.newaxis])
        combined = combined.reshape(len(self), -1)

        if len(columns) > 1:
            if len(other_columns) > 1:
                result_columns = [f"{col_self}.{col_other}" for col_self in columns for col_other in other_columns]
            else:
                result_columns = columns
        else:
            result_columns = other_columns

        result = pd.DataFrame(combined, index=a.index, columns=result_columns)

        return History(self.history_schema, result)

    def apply_on(self, other, func, *args, **kwargs):
        return History(self.history_schema,
                       func(self.data, other.data if isinstance(other, History) else other, *args, **kwargs))

    def apply(self, func: Dict[str, callable] | callable, *args, **kwargs) -> "History":
        if isinstance(func, dict):
            data = self.data

            for idx, f in func.items():
                data = data.groupby(idx, group_keys=False).apply(f, *args, **kwargs)

            schema = HistorySchema(
                index={name: self.history_schema.index.get(name) for name in data.index.names},
                columns={name: self.history_schema.columns.get(name) for name in data.columns}
            )

            return History(schema, data)
        else:
            return History(self.history_schema, self.data.apply(func, *args, **kwargs))

    def dropna(self):
        return History(self.history_schema, self.data.dropna())

    def head(self, n=5):
        return self.data.head(n)

    # endregion

    # region Properties

    # region Serializable Properties

    def to_dict(self):
        return {
            "history_schema": self.history_schema.to_dict(),
            "data": self.data.to_dict(orient="tight")
        }

    @classmethod
    def from_dict(cls, data: dict):
        history_schema = data.get("history_schema", None)
        data = data.get("data", pd.DataFrame())
        return cls(history_schema=history_schema, data=data)

    def copy(self):
        return History(history_schema=self.history_schema, data=self.data.copy())

    def store(self, storage_path, key):
        try:
            os.makedirs(storage_path + "/history")
        except FileExistsError:
            pass

        history_storage = storage_path + "/history"
        self.data.to_hdf(history_storage + f"/data.h5", key=key, mode='w', format='table')
        self.history_schema.store(history_storage + f"/history_schema", key)

    @classmethod
    def load(cls, storage_path, key):
        history_storage = storage_path + "/history"
        data = pd.read_hdf(history_storage + "/data.h5", key, mode='r')
        history_schema = HistorySchema.load(history_storage + "/history_schema", key)
        return cls(history_schema=history_schema, data=data)

    @classmethod
    def cache_exists(cls, cache_path, key):
        file_exists = os.path.exists(cache_path + f"/history/data.h5") and os.path.exists(
            cache_path + f"/history/history_schema/")
        schema_exists = os.path.exists(cache_path + f"/history/history_schema/{key}.json")
        try:
            with pd.HDFStore(cache_path + "/history/data.h5") as store:
                data_exists = f"/{key}" in store.keys()
        except (FileNotFoundError, OSError, KeyError):
            return False

        return file_exists and schema_exists and data_exists

    # endregion

    # region Inbuilt Properties

    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        return self.data[key]

    def __str__(self):
        return f"\n{self.history_schema}, \n{self.data}\n"

    # endregion

    # endregion
