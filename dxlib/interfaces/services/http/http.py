import traceback
import json
from http import HTTPStatus
from enum import Enum

from dxlib.interfaces.services.endpoint import Endpoint
from dxlib.interfaces.services.service_registry import ServiceRegistry


class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


def serialize_exception(ex: Exception):
    return {
        "type": type(ex).__name__,
        "message": str(ex),
        "traceback": traceback.format_exc()
    }


class HttpResponse:
    @staticmethod
    def error_response(message: str, status_code: int = HTTPStatus.INTERNAL_SERVER_ERROR):
        return {"error": message, "status": status_code}

    @staticmethod
    def success_response(data: dict, status_code: int = HTTPStatus.OK):
        return {"data": data, "status": status_code}


def http_handler(x):
    # no op function output handler
    return x

def http_exception_handler(e):
    if isinstance(e, ValueError):
        return HttpResponse.error_response("Bad Request: " + str(e), HTTPStatus.BAD_REQUEST)
    return HttpResponse.error_response(serialize_exception(e), HTTPStatus.INTERNAL_SERVER_ERROR)


class HttpEndpoint(Endpoint):
    def __init__(self, route, method="GET", handler=http_handler, exception_handler=http_exception_handler):
        super().__init__(route)
        self.method = method
        self.func = None
        self.handler = handler
        self.exception_handler = exception_handler

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    @classmethod
    def get(cls, route="", *args, **kwargs):
        return ServiceRegistry.decorate_endpoint(cls(route, "GET"), *args, **kwargs)

    @classmethod
    def post(cls, route="", *args, **kwargs):
        return ServiceRegistry.decorate_endpoint(cls(route, "POST"), *args, **kwargs)

    @classmethod
    def put(cls, route="", *args, **kwargs):
        return ServiceRegistry.decorate_endpoint(cls(route, "PUT"), *args, **kwargs)

    @classmethod
    def delete(cls, route="", *args, **kwargs):
        return ServiceRegistry.decorate_endpoint(cls(route, "DELETE"), *args, **kwargs)
