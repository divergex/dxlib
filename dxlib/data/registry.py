from abc import ABCMeta
from datetime import datetime

import pandas as pd

from dxlib.types import _TYPES

_REGISTRY = {}
_SERIALIZERS = {
    pd.DataFrame: lambda df: df.to_dict(orient="tight"),
    type: lambda data: str(data),
    ABCMeta: lambda data: str(data)
}

_DESERIALIZERS = {
    pd.DataFrame: lambda data: pd.DataFrame.from_dict(data, orient="tight"),
    type: lambda data: _TYPES[data],
    datetime: lambda data: datetime.fromisoformat(data),
}

class RegistryBase:
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if hasattr(cls, 'domain_cls'):
            if cls.domain_cls.__qualname__ is not None:
                _REGISTRY[cls.domain_cls.__qualname__] = cls

    @classmethod
    def serialize(cls, value):
        t = type(value)
        if registry := _REGISTRY.get(t.__qualname__):
            return registry.from_domain(value).model_dump()
        elif serializer := _SERIALIZERS.get(t):
            return serializer(value)
        elif t == dict:
            return {cls.serialize(k): cls.serialize(v) for k, v in value.items()}
        else:
            return value

    @classmethod
    def deserialize(cls, value, expected_type):
        if registry := _REGISTRY.get(expected_type):
            return registry.model_validate(value)
        elif deserializer := _DESERIALIZERS.get(expected_type):
            return deserializer(value)
        else:
            try:
                return expected_type(value)
            except TypeError:
                return value

    @staticmethod
    def registry():
        return _REGISTRY

    @classmethod
    def get_registry(cls, domain_cls):
        try:
            return _REGISTRY[domain_cls.__qualname__]
        except KeyError:
            raise KeyError(f"Data model for domain_cls {domain_cls} not in registry.")
