from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any

from dxlib.interfaces import Handler


class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


@dataclass
class Endpoint:
    url: str
    method: HttpMethod


class Http:
    @staticmethod
    def get(route: str, description: str):
        def decorator(func):
            func.method = HttpMethod.GET
            func.route = route
            func.description = description
            return func

        return decorator

    @staticmethod
    def post(route: str, description: str):
        def decorator(func):
            func.method = HttpMethod.POST
            func.route = route
            func.description = description
            return func

        return decorator

class HttpServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.interfaces = {}
        self.handlers = {}

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def add(self, route: str, callback: callable, method: str = "GET"):
        pass

    def register_handler(self, handler: Handler):
        endpoints = handler.endpoints()

        for endpoint in endpoints:
            method = getattr(handler, endpoint)
            if hasattr(method, "method"):
                route = method.route
                self.handlers[route] = handler
                self.add(route, method, method.method.value)

    def invoke(self, method_name: str, *args, **kwargs) -> Any:
        if method_name in self.interfaces:
            return self.interfaces[method_name](*args, **kwargs)
        raise ValueError(f"Method {method_name} not registered in server.")

    @property
    def url(self):
        return f"http://{self.host}:{self.port}"
