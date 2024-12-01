from abc import ABC, abstractmethod


class Service(ABC):
    def endpoints(self):
        return [method for method in dir(self) if hasattr(getattr(self, method), "method")]

    @abstractmethod
    def create_routes(self, *args, **kwargs):
        pass
