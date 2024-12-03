from abc import ABC
from dataclasses import dataclass
from typing import List

from .endpoint import Endpoint
from .service_registry import ServiceRegistry


@dataclass
class ServiceModel:
    name: str
    service_id: str
    endpoints: List[dict]
    tags: List[str]

    def to_dict(self):
        return {
            "service_id": self.service_id,
            "name": self.name,
            "endpoints": self.endpoints,
            "tags": self.tags
        }


class Service(ABC):
    """Base class for services, providing automatic registration and endpoint handling."""

    def __init__(self, name, service_id, tags=None):
        self.name = name
        self.service_id = service_id
        self.tags = tags or []

    def to_model(self):
        return ServiceModel(
            name=self.name,
            service_id=self.service_id,
            endpoints=[ServiceRegistry.signature(func) for func in ServiceRegistry.get_decorated(self)],
            tags=self.tags
        )
