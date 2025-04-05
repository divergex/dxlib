import json
from typing import Any, List

import httpx
from httpx import HTTPStatusError

from dxlib.storage import RegistryBase
from dxlib.interfaces.services import Server, ServiceData
from .mesh_service import ServiceSearch


class MeshInterface:
    def __init__(self):
        self.server: Server | None = None

    def register(self, server: Server):
        print(f"Registering server {server.host}:{server.port}")
        self.server = server

    def get_key_value(self, key: str):
        request = httpx.get(f"{self.server.host}/kv/{key}")
        request.raise_for_status()
        return request.json()

    def set_key_value(self, key: str, value: Any):
        request = httpx.put(f"{self.server.url}/kv", json={"key": key, "value": value})
        request.raise_for_status()
        return request.json()

    def register_service(self, service: ServiceData):
        try:
            request = httpx.post(f"{self.server.url}/services", data=service.__json__())
            request.raise_for_status()
            return request.json()
        except httpx.ConnectError as e:
            print("Are you sure the mesh server is running?", e)

    def deregister_service(self, name: str, service_id: str):
        request = httpx.delete(f"{self.server.url}/services/{name}/{service_id}")
        request.raise_for_status()
        return request.json()

    def get_service(self, name: str) -> List[ServiceData]:
        if not self.server.url:
            raise ValueError("No server registered, ignoring mesh.")

        request = httpx.get(f"{self.server.url}/services/{name}")
        try:
            request.raise_for_status()
        except HTTPStatusError as e:
            content = e.response.content
            error = json.loads(e.response.content).get("error", None)
            if error and "Service not found" in error:
                raise ValueError("Service not found")
            raise e
        instances = request.json()
        return [ServiceData(**instance) for instance in instances]

    def search_services(self, tag: List[str] = None):
        search = ServiceSearch(tag or [])
        request = httpx.post(f"{self.server.url}/services/search", data=search.__json__())
        request.raise_for_status()
        return request.json()
