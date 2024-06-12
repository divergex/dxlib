from __future__ import annotations

import inspect
import json
import threading
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

from .endpoint import EndpointWrapper, Method, EndpointScheme
from .handlers import HTTPHandler
from .server import ServerStatus, handle_exceptions_decorator, Server


class HTTPServer(Server):
    def __init__(
        self, handler: HTTPHandler = None, host=None, port=None, logger=None
    ):
        super().__init__(logger)
        self.handler = handler or HTTPHandler()
        self.port = port if port else self._get_free_port()
        self.host = host if host else "localhost"

        self._thread = None
        self._server: ThreadingHTTPServer | None = None
        self._error = threading.Event()
        self._running = threading.Event()

    @property
    def url(self):
        return f"http://{self.host}:{self.port}"

    def add_interface(self, interface):
        self.handler.add_interface(interface, endpoint_scheme=EndpointScheme.HTTP)

    @property
    def formatted_endpoints(self):
        formatted_endpoints = {}

        for route_name, methods in self.handler.endpoints.items():
            formatted_endpoints[route_name] = {}
            for method, (endpoint, func) in methods.items():
                formatted_endpoints[route_name][method.value] = {
                    "description": endpoint.description,
                    "params": {
                        name: str(typehint)
                        for name, typehint in dict(endpoint.params).items()
                        if name != "self"
                    },
                }

        return json.dumps(formatted_endpoints, indent=4)

    def _serve(self):
        if self._server is not None:
            raise RuntimeError("Server already started")

        class HTTPRequestHandler(SimpleHTTPRequestHandler):
            endpoints = self.handler.endpoints
            formatted_endpoints = self.formatted_endpoints

            exception_queue = self.exception_queue
            running = self._running
            logger = self.logger

            def handle_exception(self, message):
                self.exception_queue.put(message)
                self.send_response(500)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(message)}).encode())

            def parse_route(self):
                path_parts = urlparse(self.path)
                route_name = path_parts.path
                params = parse_qs(path_parts.query)

                if route_name not in self.endpoints:
                    self.send_response(404)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps({"error": "Function not found"}).encode()
                    )
                    return None, None

                return route_name, params

            def validate_endpoint(self, endpoint, func):
                if endpoint is None or func is None:
                    self.send_response(405)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps({"error": "Method Not Allowed"}).encode()
                    )

                return endpoint, func

            def call_func(
                self, func_callable: callable, endpoint: EndpointWrapper, params=None, data=None
            ):
                if params is None:
                    params = {}
                if data is None:
                    data = {}

                func_signature = endpoint.params

                required_args = [
                    arg
                    for arg, details in func_signature.items()
                    if details.default == inspect.Parameter.empty and arg != "self"
                ]

                missing_args = set(required_args) - set(data.keys() if data else [])

                if missing_args:
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps(
                            {
                                "error": f"Missing arguments",
                                "missing": list(missing_args),
                            }
                        ).encode()
                    )
                    return

                try:

                    class MethodEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if hasattr(obj, "to_dict") and callable(obj.to_dict):
                                return obj.to_dict()
                            elif hasattr(obj, "to_json") and callable(obj.to_json):
                                return obj.to_json()
                            else:
                                return super().default(obj)

                    response = func_callable(**data) if data else func_callable()

                    if isinstance(response, list) and params:
                        response = [
                            item
                            for item in response
                            if all(item.get() == v for k, v in params.items())
                        ]

                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps(
                            {"status": "success"} if response is None else response, cls=MethodEncoder
                        ).encode()
                    )

                except Exception as unknown_error:
                    raise unknown_error

            @handle_exceptions_decorator
            def do_GET(self):
                if self.path == "/":
                    response = self.formatted_endpoints
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(response.encode())
                    return

                route_name, params = self.parse_route()
                if route_name is None:
                    return
                endpoint, func = self.validate_endpoint(*self.endpoints[route_name].get(Method.GET, (None, None)))
                if endpoint is None:
                    return

                return self.call_func(func, endpoint, params)

            @handle_exceptions_decorator
            def do_POST(self):
                if self.path == "/":
                    response = self.formatted_endpoints
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(response.encode())
                    return

                route_name, params = self.parse_route()
                if route_name is None:
                    return

                endpoint, func = self.validate_endpoint(*self.endpoints[route_name].get(Method.POST, (None, None)))
                if endpoint is None:
                    return

                content_length = int(self.headers["Content-Length"])
                post_data = None

                if content_length > 0:
                    raw_post_data = self.rfile.read(content_length).decode()
                    try:
                        post_data = json.loads(raw_post_data)
                    except json.JSONDecodeError:
                        self.send_response(400)  # Bad Request
                        self.send_header("Content-type", "application/json")
                        self.end_headers()
                        self.wfile.write(
                            json.dumps({"error": "Invalid JSON data received"}).encode()
                        )
                        return

                return self.call_func(func, endpoint, params, post_data)

        HTTPRequestHandler.server = self

        try:
            with ThreadingHTTPServer(
                (self.host, self.port), HTTPRequestHandler
            ) as self._server:
                self.logger.info(f"HTTP server started. Press Ctrl+C to stop...")
                self._server.timeout = 1
                while self._running.is_set():
                    self._server.handle_request()
        except Exception as e:
            self.logger.error(f"Server error: {e}")
            self._error.set()
            self.exception_queue.put(e)
        except KeyboardInterrupt:
            self.logger.info("Server stopped by user")

    def start(self, threaded=True) -> ServerStatus:
        self.logger.info(f"HTTP server starting on address http://{self.host}:{self.port}")
        self._running.set()

        if threaded:
            self._thread = threading.Thread(target=self._serve)
            self._thread.start()
        else:
            self._serve()
        return ServerStatus.STARTED

    def stop(self) -> ServerStatus:
        if self._error.is_set():
            self.logger.warning(
                "Could not stop servers. Server might not have started properly"
            )
            return ServerStatus.ERROR

        if not self._running.is_set():
            return ServerStatus.STOPPED

        self.logger.info("Stopping HTTP server")
        self._running.clear()

        if self._server is not None and self._server.socket is not None:
            self._server = None
        if self._thread is not None and self._thread.is_alive():
            self._thread.join()
            self._thread = None

        self.logger.info("Server stopped")
        return ServerStatus.STOPPED

    @property
    def alive(self):
        return self._running.is_set() and self._thread.is_alive()

    def __del__(self):
        self.stop()
