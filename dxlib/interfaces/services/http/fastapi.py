from typing import Literal

import uvicorn
from dxlib.interfaces import HttpEndpoint
from fastapi import FastAPI
from fastapi.exceptions import FastAPIError

from dxlib.interfaces.services.protocols import Protocols
from dxlib.interfaces.services.server import Server


class FastApiServer(Server, uvicorn.Server):
    def __init__(self, host, port, log_level="info",
                 loop: Literal["none", "auto", "asyncio", "uvloop"] = "asyncio"):
        Server.__init__(self, host, port, Protocols.HTTP)
        self.health_check_url = f"{self.url}/health"

        self.app = FastAPI()
        self.config = uvicorn.Config(self.app, host=host, port=port, log_level=log_level, loop=loop)
        uvicorn.Server.__init__(self, config=self.config)

    @HttpEndpoint.get("/health")
    def health_check(self):
        return {"status": "ok"}

    def register_endpoint(self, service, path, func):
        endpoint = super().register_endpoint(service, path, func)
        try:
            self.app.add_api_route(path, func, methods=[endpoint.method])
        except FastAPIError as e:
            print(e, 'Error')
