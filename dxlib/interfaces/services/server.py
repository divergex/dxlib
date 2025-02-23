from .service import Service
from .protocols import Protocols


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
        return func.__get__(service).endpoint

    def register(self, service: Service, root_path="", *args, **kwargs):
        for key, func in service.__class__.__dict__.items():
            if hasattr(func, "endpoint"):
                endpoint = func.__get__(self).endpoint
                path = f"{root_path}/{endpoint.route}" if root_path else endpoint.route
                path = "/".join(path.split("/")).replace("//", "/")
                # add starting / if user has not added it
                if not path.startswith("/"):
                    path = f"/{path}"
                self.register_endpoint(service, path, func.__get__(service), *args, **kwargs)
