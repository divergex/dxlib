from typing import Type, ClassVar

from pydantic import BaseModel

from dxlib.core import Instrument, InstrumentStore
from dxlib.data import Serializable
from dxlib.data.serializable import DtoT, DomainT


class InstrumentDto(BaseModel, Serializable[Instrument]):
    domain_cls: ClassVar[Type[Instrument]] = Instrument

    symbol: str

    def to_domain(self) -> DomainT:
        return Instrument(
            symbol=self.symbol,
        )

    @classmethod
    def _from_domain(cls: Type[DtoT], domain_obj: DomainT) -> DtoT:
        return cls(symbol=domain_obj.symbol)


class InstrumentStoreDto(BaseModel, Serializable[InstrumentStore]):
    domain_cls: ClassVar[Type[InstrumentStore]] = InstrumentStore

    symbol: str

    def to_domain(self) -> DomainT:
        return Instrument(
            symbol=self.symbol,
        )

    @classmethod
    def _from_domain(cls: Type[DtoT], domain_obj: DomainT) -> DtoT:
        return cls(symbol=domain_obj.symbol)
