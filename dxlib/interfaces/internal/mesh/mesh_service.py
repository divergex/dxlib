from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Tuple, Set, List, Type
import json

from dxlib.interfaces.services import Service, ServiceData
from dxlib.interfaces.services.http import HttpEndpoint, http_exception_handler
from dxlib.storage import Serializable, RegistryBase, T


@dataclass
class KeyValue:
    key: str
    value: Any


@dataclass
class ServiceSearch(Serializable, metaclass=RegistryBase):
    tag: List[str] = field(default_factory=list)

    def from_dict(cls: Type[T], data: dict) -> T:
        return cls(**data)

    def to_dict(self) -> dict:
        return self.__dict__

class MeshService(Service):
    def __init__(self, name, service_id=None):
        super().__init__(name, service_id)
        self.kv_store = {}
        self.services: Dict[str, Dict[str, ServiceData]] = {}  # service name -> {service id -> instance}
        self.tagged: Dict[str, Set[Tuple[str, str]]] = {}  # tag -> set of (name, id)
        self.service_index: Dict[Tuple[str, str], ServiceData] = {}

    @HttpEndpoint.post("/services")
    def register_service(self, service: ServiceData):
        """
        Updates existing services endpoints, by adding to the service dictionary and the service index.
        """
        if service.name not in self.services:
            self.services[service.name] = {}
        if service.service_id not in self.services[service.name]:
            self.service_index[(service.name, service.service_id)] = service

        if service.service_id not in self.services[service.name]:
            self.services[service.name][service.service_id] = service
        else:
            self.services[service.name][service.service_id].update(service)

        for tag in service.tags:
            if tag not in self.tagged:
                self.tagged[tag] = set()
            self.tagged[tag].add((service.name, service.service_id))

    def _search_tag(self, tag: str):
        return [self.service_index[(name, service_id)] for name, service_id in self.tagged.get(tag, set())]

    @HttpEndpoint.post("/services/search")
    def search_services(self, search: ServiceSearch):
        """Search for services by tag."""
        results = defaultdict(dict)
        if not search.tag:
            return self.services
        for service in self.service_index.values():
            if any(tag in service.tags for tag in search.tag):
                results[name].update({service_id: service})
        if not results:
            raise Exception("No services found with the given tag")
        return results

    @HttpEndpoint.get("/services/{name}")
    def get_services(self, name: str):
        """Get all instances of a service by name."""
        if name not in self.services:
            raise Exception("Service not found")
        return list(self.services.get(name, {}).values())

    @HttpEndpoint.delete("/services/{name}/{service_id}")
    def deregister_service(self, name: str, service_id: str):
        """Deregister a service instance by name and ID."""
        if name in self.services and service_id in self.services[name]:
            service = self.services[name].pop(service_id)
            if not self.services[name]:
                del self.services[name]

            del self.service_index[(name, service_id)]

            for tag in service.tags:
                if tag in self.tagged:
                    self.tagged[tag].remove((name, service_id))
                    if not self.tagged[tag]:
                        del self.tagged[tag]

        return {"message": "Service deregistered successfully"}

    @HttpEndpoint.get("/discovery/{name}")
    def discover_service(self, name: str):
        """Discover endpoints for a given service name."""
        if name not in self.services:
            raise Exception("Service not found")
        return list(self.services.get(name, {}).values())

    @HttpEndpoint.put("/kv")
    def set_key_value(self, data: KeyValue):
        """Set a key-value pair."""
        self.kv_store[data.key] = data.value
        return data

    @HttpEndpoint.get("/kv/{key}")
    def get_key_value(self, key: str):
        """Retrieve a value by key."""
        if key not in self.kv_store:
            raise Exception("Key not found")
        return self.kv_store[key]
