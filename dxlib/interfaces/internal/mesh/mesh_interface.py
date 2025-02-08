from typing import Any

import httpx

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


    def search_services(self, tag: str):
        request = httpx.get(f"{self.server.url}/services/search?tag={tag}")
        request.raise_for_status()
        return request.json()
