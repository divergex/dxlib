from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum

from dxlib.interfaces.servers.endpoint import Endpoint
from dxlib.interfaces.servers.service_registry import ServiceRegistry


class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


class HttpEndpoint(Endpoint):
    def __init__(self, route, method="GET"):
        super().__init__(route)
        self.method = method

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    @classmethod
    def get(cls, route="", *args, **kwargs):
        return ServiceRegistry.decorate_endpoint(cls(route, "GET"))

    @classmethod
    def post(cls, route="", *args, **kwargs):
        return ServiceRegistry.decorate_endpoint(cls(route, "POST"))

    @classmethod
    def put(cls, route="", *args, **kwargs):
        return ServiceRegistry.decorate_endpoint(cls(route, "PUT"))

    @classmethod
    def delete(cls, route="", *args, **kwargs):
        return ServiceRegistry.decorate_endpoint(cls(route, "DELETE"))
