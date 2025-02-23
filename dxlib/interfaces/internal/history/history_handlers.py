# from typing import Dict, List, Optional, Union
# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
#
# from dxlib.core import History
# from dxlib.interfaces import Service
#
#
# def slice_to_dict(slice_obj: Optional[slice]) -> Optional[dict]:
#     if slice_obj is None:
#         return None
#     return {"start": slice_obj.start, "stop": slice_obj.stop, "step": slice_obj.step}
#
#
# def dict_to_slice(data: Optional[dict]) -> Optional[slice]:
#     if data is None:
#         return None
#     return slice(data.get("start"), data.get("stop"), data.get("step"))
#
#
# class IndexRequest(BaseModel):
#     index: Optional[Dict[str, Union[dict, List[int]]]] = None
#     columns: Optional[Union[List[str], str]] = None
#     raw: Optional[bool] = False
#
#
# class HistoryResponse(BaseModel):
#     data: dict
#     schema: dict
#
#
# class HistoryHttpHandler(Service):
#     def __init__(self):
#         self.store: Optional[History] = None
#
#     def register(self, history: History, identifier: str):
#         self.store = history
#
#     def create_routes(self, app: FastAPI):
#         @app.post("/get", response_model=HistoryResponse, description="Get historical data for a symbol")
#         async def get(request: IndexRequest):
#             if self.store is None:
#                 raise HTTPException(status_code=404, detail="History store not registered")
#
#             # Convert index dictionaries back to slices
#             parsed_index = {
#                 key: dict_to_slice(value) if isinstance(value, dict) else value
#                 for key, value in (request.index or {}).items()
#             }
#
#             history_data = self.store.get(parsed_index, request.columns, request.raw)
#
#             return HistoryResponse(**history_data.to_dict())
