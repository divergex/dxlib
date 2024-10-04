from typing import Dict, Type, List

from ...storage import Serializable, RegistryBase


class HistorySchema(Serializable, metaclass=RegistryBase):
    """
    A schema is the structure of a data set.
    It contains the index names mapped to their respective types and levels,
    as well as the column names mapped to their types.
    """
    # region Custom Properties

    @property
    def index_names(self) -> List[str]:
        return list(self.index.keys())

    @property
    def column_names(self) -> List[str]:
        return list(self.columns.keys())

    def to_dict(self) -> dict:
        return {
            "index": {name: type_.__name__ for name, type_ in self.index.items()},
            "columns": {name: type_.__name__ for name, type_ in self.columns.items()}
        }

    @classmethod
    def from_dict(cls, data: dict) -> "HistorySchema":
        return cls(
            index={name: RegistryBase.get(type_) for name, type_ in data["index"].items()},
            columns={name: RegistryBase.get(type_) for name, type_ in data["columns"].items()}
        )

    def copy(self) -> "HistorySchema":
        return HistorySchema(
            index={name: type_ for name, type_ in self.index.items()},
            columns={name: type_ for name, type_ in self.columns.items()}
        )

    # endregion

    # region Inbuilt Properties

    def __init__(self, index: Dict[str, Type] = None, columns: Dict[str, Type] = None):
        self.index: Dict[str, Type] = index
        self.columns: Dict[str, Type] = columns

    def __eq__(self, other):
        return self.index == other.index and self.columns == other.columns

    # endregion
