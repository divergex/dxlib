from typing import Dict, Type, List

from dxlib.types import TypeRegistry

class HistorySchema(TypeRegistry):
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

    # region Inbuilt Properties

    def __eq__(self, other):
        return self.index == other.index and self.columns == other.columns

    def __str__(self):
        return f"Index: {self.index}, \nColumns: {self.columns}"

    # endregion
