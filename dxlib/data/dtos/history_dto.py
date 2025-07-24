from typing import Dict, Type, Optional, ClassVar

import pandas as pd
from pydantic import Field, BaseModel

from dxlib.history import History, HistorySchema

from ..serializable import Serializable


class HistorySchemaDto(BaseModel, Serializable[HistorySchema]):
    domain_cls: ClassVar[Type[HistorySchema]] = HistorySchema

    index: Optional[Dict[str, str]] = Field(default=None, description="Dict of index names to types")
    columns: Optional[Dict[str, str]] = Field(default=None, description="Dict of column names to types")

    def to_domain(self) -> HistorySchema:
        return HistorySchema(
            index={k: self.deserialize(v, type) for k, v in self.index.items()},
            columns={k: self.deserialize(v, type) for k, v in self.columns.items()},
        )

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
        schema = self.history_schema.to_domain()
        df = self.deserialize(self.data, dict)

        for col, expected_type in schema.columns.items():
            df[col] = df[col].apply(lambda x: self.deserialize(x, expected_type))

        if isinstance(df.index, pd.MultiIndex):
            new_levels = []
            for level, (name, expected_type) in zip(df.index.levels, schema.index.items()):
                new_level = [self.deserialize(val, expected_type) for val in level]
                new_levels.append(pd.Index(new_level, name=name))
            df.index = df.index.set_levels(new_levels)
        else:
            # Single index
            index_name, expected_type = next(iter(schema.index.items()))
            new_index = [self.deserialize(val, expected_type) for val in df.index]
            df.index = pd.Index(new_index, name=index_name)

        return History(schema, df)

    @classmethod
    def _from_domain(cls, domain_obj: History) -> "HistoryDto":
        return cls(
            data=cls.serialize(domain_obj.data),
            history_schema=cls.serialize(domain_obj.history_schema),
        )
