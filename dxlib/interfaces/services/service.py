from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Set, Dict

from .service_registry import ServiceRegistry


@dataclass
class ServiceData:
    """
    For internal data representation, such as endpoints available, tags matching the service's functionality, origin,
    and other relevant information to reference and identifty services.
    """
    name: str
    service_id: str
    endpoints: Dict[str, Dict[str, dict]] = field(default_factory=dict)
    tags: Set[str] = field(default_factory=set)
    signature: Dict[str, dict] = field(default_factory=dict)

    def to_dict(self):
        return {
            "service_id": self.service_id,
            "name": self.name,
            "endpoints": self.endpoints,
            "signature": self.signature,
            "tags": self.tags
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            service_id=data["service_id"],
            name=data["name"],
            endpoints=data["endpoints"],
            signature=data["signature"],
            tags=data["tags"]
        )

    def update(self, service: "ServiceData"):
        self.tags.update(service.tags)
        self.signature.update(service.signature)
        for endpoint, details in service.endpoints.items():
            if endpoint not in self.endpoints:
                self.endpoints[endpoint] = {}
            self.endpoints[endpoint].update(details)


class Service(ABC):
    """Base class for services, providing automatic registration and endpoint handling."""

    def __init__(self, name, service_id, tags=None):
        self.name = name
        self.service_id = service_id
        self.tags = tags or []

    def endpoints(self, root_path=""):
        endpoints = defaultdict(dict)
        for key, func in self.__class__.__dict__.items():
            if hasattr(func, "endpoint"):
                endpoint = func.__get__(self).endpoint
                path = endpoint.route
                path = "/".join(path.split("/")).replace("//", "/")
                if not path.startswith("/"):
                    path = f"/{path}"
                route = f"{root_path}/{endpoint.route.lstrip('/')}"
                endpoints[route].update({f"{endpoint.method}": {
                    "path": path,
                    "method": endpoint.method,
                    "handler": func.__name__,
                    "exception_handler": endpoint.exception_handler.__name__
                }})
        return endpoints

        path = endpoint.route
    def data(self, root_path=""):
        signatures = [ServiceRegistry.signature(func) for func in ServiceRegistry.get_decorated(self)]
        return ServiceData(
            name=self.name,
            service_id=self.service_id,
            endpoints=self.endpoints(root_path),
            tags=self.tags,
            signature={signature["name"]: signature for signature in signatures}
        )
