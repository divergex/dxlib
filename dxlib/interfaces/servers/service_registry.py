from abc import ABCMeta


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
            endpoint.func = func
            func.endpoint = endpoint
            return func

        return decorator