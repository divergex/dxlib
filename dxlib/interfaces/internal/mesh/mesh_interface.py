import json
from typing import Any

import httpx
from httpx import HTTPStatusError

from dxlib.interfaces.services import Server, ServiceModel


class MeshInterface:
    def __init__(self):
        self.server: Server | None = None

    def register(self, server: Server):
        self.server = server

    def get_key_value(self, key: str):
        request = httpx.get(f"{self.server.host}/kv/{key}")
        request.raise_for_status()
        return request.json()

    def set_key_value(self, key: str, value: Any):
        request = httpx.put(f"{self.server.url}/kv", json={"key": key, "value": value})
        request.raise_for_status()
        return request.json()

    def register_service(self, service: ServiceModel):
        try:
            request = httpx.post(f"{self.server.url}/services", json=service.to_dict())
            request.raise_for_status()
            return request.json()
        except httpx.ConnectError as e:
            print("Are you sure the mesh server is running?", e)

    def deregister_service(self, name: str, service_id: str):
        request = httpx.delete(f"{self.server.url}/services/{name}/{service_id}")
        request.raise_for_status()
        return request.json()

    def get_service(self, name: str):
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
        return request.json()

    def search_services(self, tag: str):
        request = httpx.get(f"{self.server.url}/services/search?tag={tag}")
        request.raise_for_status()
        return request.json()
