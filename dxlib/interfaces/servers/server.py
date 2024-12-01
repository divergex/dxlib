from abc import ABC

from dxlib.interfaces.servers.protocols import Protocols


class Service(ABC):
    """Base class for services, providing automatic registration and endpoint handling."""

    def __init__(self, name, service_id, tags=None):
        self.name = name
        self.service_id = service_id
        self.tags = tags or []


class Server:
    def __init__(self, host, port, protocol: Protocols = Protocols.HTTP):
        self.protocol = protocol
        self.host = host
        self.port = port

        self.endpoints = {}

    @property
    def url(self):
        return f"{self.protocol.value}://{self.host}:{self.port}"

    def register_endpoint(self, service, path, func):
        self.endpoints[path] = func

    def register(self, service: Service, root_path="/"):
        for key, func in service.__class__.__dict__.items():
            if hasattr(func, "endpoint"):
                endpoint = func.__get__(self).endpoint
                path = f"{root_path}/{endpoint.route}"
                path = "/".join(path.split("/")).replace("//", "/")
                self.register_endpoint(service, path, func.__get__(service))