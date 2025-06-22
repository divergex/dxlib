from abc import abstractmethod, ABCMeta
from datetime import datetime
from numbers import Number
from typing import Type, TypeVar, Generic, ClassVar, Any, Dict

import pandas as pd
from pandas import Timestamp

_REGISTRY = {}
_SERIALIZERS = {
    pd.DataFrame: lambda df: df.to_dict(orient="tight"),
    type: lambda data: str(data),
    ABCMeta: lambda data: str(data)
}

_TYPES: Dict[str, Type] = {str(t): t for t in [int, float, str, bool, datetime, Timestamp, Number]}
_DESERIALIZERS = {
    pd.DataFrame: lambda data: pd.DataFrame.from_dict(data, orient="tight"),
    type: lambda data: _TYPES[data],
}

class TypeRegistry:
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        _TYPES[str(cls)] = cls

    @classmethod
    def get(cls, name):
        return _TYPES[name]

    @classmethod
    def register(cls, obj):
        _REGISTRY[str(obj)] = obj

class RegistryBase:
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if hasattr(cls, 'domain_cls'):
            if cls.domain_cls.__name__ is not None:
                _REGISTRY[cls.domain_cls.__name__] = cls

    @classmethod
    def serialize(cls, value):
        t = type(value)
        if registry := _REGISTRY.get(t.__name__):
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
            return value

    @staticmethod
    def registry():
        return _REGISTRY

DomainT = TypeVar('DomainT')
DtoT = TypeVar('DtoT', bound='Serializable')

class Serializable(Generic[DomainT], RegistryBase):
    domain_cls: ClassVar[Any]  # Should be a type of DomainT

    @abstractmethod
    def to_domain(self) -> DomainT:
        pass

    @classmethod
    def from_domain(cls: Type[DtoT], domain_obj: DomainT) -> DtoT:
        if not isinstance(domain_obj, cls.domain_cls):
            raise TypeError(f"{cls.__name__} expects instance of {cls.domain_cls.__name__}")
        return cls._from_domain(domain_obj)

    @classmethod
    @abstractmethod
    def _from_domain(cls: Type[DtoT], domain_obj: DomainT) -> DtoT:
        pass

    @abstractmethod
    def model_dump(self) -> dict:
        pass

    @abstractmethod
    def model_dump_json(self) -> str:
        pass

    @abstractmethod
    def model_validate_json(self) -> DtoT:
        pass
