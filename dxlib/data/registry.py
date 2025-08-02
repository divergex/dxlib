from abc import ABCMeta
from datetime import datetime

import pandas as pd

from dxlib.types import _TYPES

_REGISTRY = {}
_SERIALIZERS = {
    pd.DataFrame: lambda df: df.to_dict("tight"),
    type: lambda data: str(data),
    ABCMeta: lambda data: str(data),
    datetime: lambda data: str(data),
}

_DESERIALIZERS = {
    pd.DataFrame: lambda data: pd.DataFrame.from_dict(data, orient="tight"),
    type: lambda data: _TYPES[data],
    datetime: lambda data: datetime.fromisoformat(data),
}

class Registry:
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
            return cls.serialize(serializer(value))
        elif t == dict:
            return {cls.serialize(k): cls.serialize(v) for k, v in value.items()}
        elif isinstance(value, (list, tuple)):
            return [cls.serialize(item) for item in value]
        else:
            return value

    @classmethod
    def deserialize(cls, value, expected_type):
        if registry := _REGISTRY.get(expected_type.__qualname__):
            return registry.model_validate(value).to_domain()
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
    def get(cls, domain_cls):
        try:
            cls_name = domain_cls.__qualname__ if isinstance(domain_cls, type) else domain_cls.__class__.__qualname__
            return _REGISTRY[cls_name]
        except KeyError:
            raise KeyError(f"Data model for domain_cls {domain_cls} not in registry.")

    @classmethod
    def from_domain(cls, domain):
        assert not isinstance(domain, type), "Pass a domain instance instead of its definition."
        try:
            return _REGISTRY[domain.__class__.__qualname__].from_domain(domain)
        except KeyError:
            raise KeyError(f"Data model for domain {domain} not in registry.")
