# from typing import Dict, List
#
# import httpx
#
# from dxlib import HistorySchema
# from dxlib.core import History
# from dxlib.interfaces import Interface
# from dxlib.interfaces.services.http.http import Endpoint, HttpMethod
#
#
# class HistoryHttpInterface(History, Interface):
#     def __init__(self):
#         super().__init__()
#         self.services: List[HttpServer] = []
#         self.headers = {"Content-Type": "application/json"}
#
#     def register(self, server):
#         self.services.append(server)
#
#     def locate(self, method) -> Endpoint:
#         return Endpoint(url=self.services[0].url, method=method)
#
#     @Http.post(route="/get", description="Get historical data for a symbol")
#     def get(self, index: Dict[str, slice | list] = None, columns: List[str] | str = None, raw=False) -> History:
#         endpoint = self.locate(HttpMethod.POST)
#         body = {"index": index, "columns": columns, "raw": raw}
#         response = httpx.post(endpoint.url + "/get", headers=self.headers, json=body)
#
#         if response.status_code == 200:
#             schema = HistorySchema.from_dict(response.json().get("schema"))
#             return History.from_dict({"data": response.json().get("data"), "schema": schema})
#         else:
#             raise Exception(f"Failed to get data: {response.status_code}")
