import inspect
from abc import ABCMeta
from typing import List
from functools import wraps

from .endpoint import Endpoint


class ServiceRegistry(ABCMeta):
    @classmethod
    def decorate_endpoint(cls, endpoint):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            wrapper.endpoint = endpoint
            return wrapper
        return decorator

    @staticmethod
    def get_decorated(service: any) -> List[callable]:
        return [getattr(service, attr) for attr in dir(service) if hasattr(getattr(service, attr), "endpoint")]

    @staticmethod
    def serializer():
        """Json serializer to handle additional types """

    @staticmethod
    def signature(func):
        signature = inspect.signature(func)

        return {
            "name": func.__name__,
            "doc": func.__doc__,
            "parameters": [
                {"name": str(param.name), "type": str(param.annotation.__name__), "default": str(param.default)}
                for param in signature.parameters.values()
            ],
            "return_type": str(signature.return_annotation)
        }
