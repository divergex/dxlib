from typing import Type

from pydantic import BaseModel

from dxlib import Security
from dxlib.data import Serializable
from dxlib.data.serializable import DtoT, DomainT


class SecurityDto(BaseModel, Serializable[Security]):
    domain_cls = Security

    symbol: str

    def to_domain(self) -> DomainT:
        return Security(
            symbol=self.symbol,
        )

    @classmethod
    def _from_domain(cls: Type[DtoT], domain_obj: DomainT) -> DtoT:
        return cls(symbol=domain_obj.symbol)
