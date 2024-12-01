import threading
from typing import Literal

import uvicorn
from fastapi import FastAPI
from fastapi.exceptions import FastAPIError

from dxlib.interfaces.servers.protocols import Protocols
from dxlib.interfaces.servers.server import Server


class FastApiServer(Server):
    def __init__(self, host, port, log_level="info",
                 loop: Literal["none", "auto", "asyncio", "uvloop"] = "asyncio"):
        super().__init__(host, port, Protocols.HTTP)
        self.health_check_url = f"{self.url}/health"

        self.app = FastAPI()
        self.config = uvicorn.Config(self.app, host=host, port=port, log_level=log_level, loop=loop)

    def run(self, threaded=False):
        def start():
            uvicorn.run(self.app, host=self.host, port=self.port)

        if threaded:
            t = threading.Thread(target=start)
            t.start()
            return t
        else:
            start()

    def register_endpoint(self, service, path, func):
        super().register_endpoint(service, path, func)
        endpoint = func.__get__(service).endpoint
        try:
            self.app.add_api_route(path, func, methods=[endpoint.method])
        except FastAPIError as e:
            print(e, 'Error')
