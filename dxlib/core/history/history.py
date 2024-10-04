from typing import Dict, List

import pandas as pd

from ...storage import Serializable, RegistryBase
from .history_schema import HistorySchema


class History(Serializable, metaclass=RegistryBase):
    """
    A history is a term used to describe a collection of data points.

    A history in the context of this library is extended to include the concept of a time series.
    Usually the data points are indexed by time and sometimes by a security.

    The main purpose of a history is to provide common methods to manipulate and analyze the data, as well as context.
    This is useful for easily storing, retrieving, backtesting and networking data.
    """
    def __init__(self, schema: HistorySchema = None,
                 data: pd.DataFrame | dict | list = None):
        """
        A history is a collection of dense mutable data points.

        It is described by a schema, that can be used to navigate and manipulate the data,
        as well as to provide context.

        The schema is necessary for any operations on data, but is not required for initialization,
        as a history instance can be used as a placeholder for data.

        Args:
            schema (HistorySchema): The schema that describes the data.
            data (pd.DataFrame | dict | list): The data for which this history is a container. Can be used elsewhere.

        Returns:
            History: A history instance.
        """
        self.schema = schema
        self.data = None

        if isinstance(data, pd.DataFrame):
            self.data: pd.DataFrame = data
        elif isinstance(data, dict):
            self.data: pd.DataFrame = pd.DataFrame.from_dict(data, orient="tight")
        elif isinstance(data, list):
            self.data: pd.DataFrame = pd.DataFrame(data)

        # test if self.data index have correct names
        if self.schema is not None and self.data is not None:
            if self.schema.index and self.schema.index.keys() != set(self.data.index.names):
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

    def levels(self, names: List[str] | str) -> list | Dict[str, list]:
        """
        Get the levels of the index by name.

        Args:
            names (List[str] | str): The names of the index levels.

        Returns:
            list | Dict[str, list]: The levels of the index.
        """
        if isinstance(names, str):
            return self.data.index.get_level_values(self.idx(names)).unique().tolist()
        else:
            return {name: self.levels(name) for name in names}

    # endregion

    # region Manipulation

    def add(self, other: "History", keep="first") -> "History":
        """
        Complements current history with another histories content. Ignores repeated data.

        Args:
            other (History): The history to add to this history.
            keep (Literal["first", "last"]): The strategy to use when keeping repeated data.

        Returns:
            History: This history, now with the data of the other history.
        """
        if self.schema != other.schema:
            raise ValueError("The schemas of the histories do not match.")

        self.data = pd.concat([self.data, other.data])
        self.data = self.data[~self.data.index.duplicated(keep=keep)]
        return self

    def extend(self, other: "History") -> "History":
        """
        Extends current history columns with another histories columns.

        Args:
            other (History): The history to extend this history with.

        Returns:
            History: This history, now the extended column set of the other history.
        """
        if self.schema.index != other.schema.index:
            raise ValueError("The indexes of the histories do not match.")

        self.schema.columns.update(other.schema.columns)

        # Now, add the columns of the other history to this history.
        #
        # For same index, the columns are added.
        # For different index, new and existing columns are extended with NaN values.
        self.data = pd.concat([self.data, other.data], axis=1)
        # use groupby to avoid duplicates
        self.data = self.data.groupby(level=self.data.index.names).first()
        # drop duplicate columns
        self.data = self.data.T.groupby(self.data.columns).first().T
        return self

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
        columns = columns or slice(None)
        idx = pd.IndexSlice
        slicers = [index.get(level, slice(None)) for level in self.data.index.names]
        data = self.data.sort_index().loc[idx[tuple(slicers)], columns]

        return data if raw else History(schema=self.schema, data=data)

    # endregion

    # region Properties

    # region Custom Properties

    def to_dict(self):
        return {
            "schema": self.schema.to_dict(),
            "data": self.data.to_dict(orient="tight")
        }

    @classmethod
    def from_dict(cls, data: dict):
        schema = data.get("schema", None)
        data = data.get("data", pd.DataFrame())
        return cls(schema=schema, data=data)

    def copy(self):
        return History(schema=self.schema, data=self.data.copy())

    # endregion

    # region Inbuilt Properties

    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        return self.data[key]

    def __iadd__(self, other):
        if isinstance(other, History):
            return self.add(other)
        else:
            raise ValueError("Can only add History objects.")

    def __add__(self, other):
        history = self.copy()

        if isinstance(other, History):
            return history.add(other)
        else:
            raise ValueError("Can only add History objects.")

    def __repr__(self):
        return f"History(schema={self.schema}, data={self.data})"

    # endregion

    # endregion
