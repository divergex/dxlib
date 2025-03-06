import inspect
from abc import ABCMeta
from typing import List
from functools import wraps

from .endpoint import Endpoint


class ServiceRegistry(ABCMeta):
    # def __new__(cls, name, bases, dct):
    #     new_class = super().__new__(cls, name, bases, dct)
    #
    #     for key, value in dct.items():
    #         if hasattr(value, "endpoint"):
    #             endpoint = getattr(value, "endpoint")
    #             cls.register_endpoint(new_class, endpoint)
    #
    #     return new_class
    #
    # @staticmethod
    # def register_endpoint(cls, endpoint: Endpoint):
    #     @wraps(endpoint.func)
    #     def wrapper(*args, **kwargs):
    #         return endpoint.func(*args, **kwargs)
    #     setattr(cls, endpoint.route, wrapper)
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
