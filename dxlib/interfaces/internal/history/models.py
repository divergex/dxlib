from dataclasses import dataclass
from typing import Dict

from dxlib.core import History


@dataclass
class HistorySchemaModel:
    index: Dict[str, str]
    columns: Dict[str, str]


@dataclass
class HistoryModel:
    history_schema: HistorySchemaModel
    data: dict

    def to_history(self) -> History:
        return History.from_dict(self.data)
