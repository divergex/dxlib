import json
import os
from typing import Dict, Type, List

import pandas as pd

from dxlib.storage import Serializable, RegistryBase


class HistorySchema(Serializable, metaclass=RegistryBase):
    """
    A schema is the structure of a data set.
    It contains the index names mapped to their respective types and levels,
    as well as the column names mapped to their types.
    """
    def __init__(self, index: Dict[str, Type] = None, columns: Dict[str, Type] = None):
        self.index: Dict[str, Type] = index  # name = [level1, level2, ...], type = ['str', 'int', ...]
        self.columns: Dict[str, Type] = columns

    # region Custom Attributes

    @property
    def index_names(self) -> List[str]:
        return list(self.index.keys())

    @property
    def column_names(self) -> List[str]:
        return list(self.columns.keys())

    # endregion

    # region Manipulation Methods

    def copy(self) -> "HistorySchema":
        return HistorySchema(
            index={name: type_ for name, type_ in self.index.items()},
            columns={name: type_ for name, type_ in self.columns.items()}
        )

    def in_index(self, name: str) -> bool:
        return name in self.index

    def in_column(self, name: str) -> bool:
        return name in self.columns

    # endregion

    # region Serialization

    def to_dict(self) -> dict:
        return {
            "index": {name: type_.__name__ for name, type_ in self.index.items()},
            "columns": {name: type_.__name__ for name, type_ in self.columns.items()}
        }

    @classmethod
    def from_dict(cls, data: dict) -> "HistorySchema":
        return cls(
            index={key: cls.REGISTRY.get(type_) for key, type_ in data["index"].items()},
            columns={key: cls.REGISTRY.get(type_) for key, type_ in data["columns"].items()}
        )

    @classmethod
    def from_df(cls, df: pd.DataFrame) -> "HistorySchema":
        return cls(
            index={name: type_ for name, type_ in zip(df.index.names, df.index.dtypes if isinstance(df.index, pd.MultiIndex) else [df.index.dtype])},
            columns={name: type_ for name, type_ in zip(df.columns, df.dtypes)}
        )

    def store(self, storage_path, key):
        try:
            os.makedirs(storage_path + "/history")
        except FileExistsError:
            pass

        schema_data = {
            'index': {k: v.__name__ for k, v in self.index.items()},
            'columns': {k: v.__name__ for k, v in self.columns.items()}
        }

        with open(f'{storage_path}/{key}.json', 'wb') as file:
            json_data = json.dumps(schema_data)
            file.write(json_data.encode('utf-8'))

    @classmethod
    def load(cls, cache_path, key):
        # Load from JSON file
        with open(f'{cache_path}/{key}.json', 'r') as json_file:
            schema_data = json.load(json_file)

        index = {key: cls.REGISTRY.get(type_) for key, type_ in schema_data['index'].items()}
        columns = {key: cls.REGISTRY.get(type_) for key, type_ in schema_data['columns'].items()}

        return cls(index=index, columns=columns)

    # endregion

    # region Inbuilt Properties

    def __eq__(self, other):
        return self.index == other.index and self.columns == other.columns

    def __str__(self):
        return f"Index: {self.index}, \nColumns: {self.columns}"

    # endregion
