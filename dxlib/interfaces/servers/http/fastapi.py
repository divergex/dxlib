import uvicorn
from fastapi import FastAPI

from .http import HttpServer
from ... import Handler


class FastApiServer(HttpServer):
    def __init__(self, host: str, port: int):
        self.app = FastAPI()
        self.config = uvicorn.Config(self.app, host=host, port=port, log_level="info", loop="asyncio")
        HttpServer.__init__(self, host, port)
        self.server = uvicorn.Server(self.config)

    def start(self):
        self.server.run()

    def add(self, route: str, callback: callable, method: str = "GET"):
        self.app.add_api_route(route, callback, methods=[method])
        return self

    def register_handler(self, handler: Handler):
        handler.create_routes(self.app)
        super().register_handler(handler)

    def setup(self):
        # for each self.handler dict (route -> handler), add the route to the FastAPI app
        for route, handler in self.handlers.items():
            self.add(route, handler)
