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

    def _deserialize_tight_dataframe(self, payload: dict, schema: HistorySchema) -> pd.DataFrame:
        index_schema_items = list(schema.index.items())  # List of (name, Type)

        raw_index = payload["index"]  # List of lists if MultiIndex, else list

        if len(index_schema_items) > 1:
            tuples = [
                tuple(
                    self.deserialize(val, expected_type)
                    for val, (_, expected_type) in zip(entry, index_schema_items)
                )
                for entry in raw_index
            ]
            index = pd.MultiIndex.from_tuples(tuples, names=schema.index.keys())
        else:
            # Single Index
            name, expected_type = index_schema_items[0]
            index_vals = [self.deserialize(val, expected_type) for val in raw_index]
            index = pd.Index(index_vals, name=name)

        df = pd.DataFrame(data=payload["data"], index=index, columns=payload["columns"])

        for col, expected_type in schema.columns.items():
            if col in df.columns:
                df[col] = df[col].apply(lambda x: self.deserialize(x, expected_type))

        return df

    def to_domain(self) -> History:
        schema = self.history_schema.to_domain()
        df = self._deserialize_tight_dataframe(self.data, schema)
        return History(schema, df)

    @classmethod
    def _from_domain(cls, domain_obj: History) -> "HistoryDto":
        return cls(
            data=cls.serialize(domain_obj.data),
            history_schema=cls.serialize(domain_obj.history_schema),
        )
