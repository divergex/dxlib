from typing import Dict, Type, Optional, ClassVar

from pydantic import Field, BaseModel

from dxlib import History
from dxlib.data import Serializable
from dxlib.history import HistorySchema


class HistorySchemaDto(BaseModel, Serializable[HistorySchema]):
    domain_cls: ClassVar[Type[HistorySchema]] = HistorySchema

    index: Optional[Dict[str, str]] = Field(default=None, description="Dict of index names to types")
    columns: Optional[Dict[str, str]] = Field(default=None, description="Dict of column names to types")

    def to_domain(self) -> HistorySchema:
        return HistorySchema(index=self.index, columns=self.columns)

    @classmethod
    def _from_domain(cls, domain_obj: HistorySchema) -> "HistorySchemaDto":
        return cls(
            index=cls.serialize(domain_obj.index),
            columns=cls.serialize(domain_obj.columns),
        )

class HistoryDto(BaseModel, Serializable[History]):
    domain_cls: ClassVar[Type[History]] = History

    data: dict = Field()
    history_schema: HistorySchemaDto = Field()

    def to_domain(self) -> History:
        return History(self.data, self.history_schema.to_domain())

    @classmethod
    def _from_domain(cls, domain_obj: History) -> "HistoryDto":
        return cls(
            data=cls.serialize(domain_obj.data),
            history_schema=cls.serialize(domain_obj.history_schema),
        )
