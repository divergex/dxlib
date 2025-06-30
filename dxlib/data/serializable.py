from abc import abstractmethod
from typing import TypeVar, Generic, ClassVar, Any, Type

from .registry import Registry

DomainT = TypeVar('DomainT')
DtoT = TypeVar('DtoT', bound='Serializable')

class Serializable(Generic[DomainT], Registry):
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
