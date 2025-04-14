from typing import Literal
from functools import wraps

import uvicorn
from starlette.responses import JSONResponse
from fastapi import FastAPI
from fastapi.exceptions import FastAPIError

from dxlib.interfaces import HttpEndpoint, Service
from dxlib.interfaces.services.protocols import Protocols
from dxlib.interfaces.services.server import Server


def exception_handler(e):
    raise HTTPException(status_code=500, detail=e)


class FastApiServer(Server, uvicorn.Server):
    def __init__(self,
                 host,
                 port,
                 config=None,
                 log_level="info",
                 loop: Literal["none", "auto", "asyncio", "uvloop"] = "asyncio",
                 *args,
                 **kwargs
                 ):
        Server.__init__(self, host, port, Protocols.HTTP)
        self.health_check_url = f"{self.url}/health"

        self.app = FastAPI(*args, **kwargs)
        self.config = config or uvicorn.Config(self.app, host=host, port=port, log_level=log_level, loop=loop)
        uvicorn.Server.__init__(self, config=self.config)

    @HttpEndpoint.get("/health")
    def health_check(self):
        return {"status": "ok"}

    def register_endpoint(self, service, path, func, router=None):
        # self.endpoints[path] = func
        # wrap function with endpoints handler and exception handler
        def wrapped(*args, **kwargs):
            try:
                return endpoint.handler(func(*args, **kwargs))
            except Exception as e:
                response = endpoint.exception_handler(e)
                return JSONResponse(content=response, status_code=response.get("status", 200))
        # update wrapped signature to match the original function
        wrapped = wraps(func)(wrapped)

        self.endpoints[path] = wrapped
        endpoint = func.__get__(service).endpoint
        try:
            if router:
                router.add_api_route(path, wrapped, methods=[endpoint.method])
            else:
                self.app.add_api_route(path, wrapped, methods=[endpoint.method])
        except FastAPIError as e:
            print(e, 'Error')

    def include_router(self, router):
        self.app.include_router(router)

    def register(self, service: Service, root_path="", external_router=None, **kwargs):
        super().register(service, root_path, router=external_router)
        if hasattr(service, "router"):
            self.include_router(service.router)
        if external_router:
            self.include_router(external_router)

    def stop(self):
        self.should_exit = True
