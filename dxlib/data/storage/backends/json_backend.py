import os
import json

from ..stored_attribute import AttributeFormat
from ..backend import StorageBackend


class JSONBackend(StorageBackend):
    @property
    def supported_formats(self) -> set[AttributeFormat]:
        return {AttributeFormat.JSON}

    def __init__(self, root: str):
        super().__init__()
        self.root = root
        os.makedirs(root, exist_ok=True)

    def _path(self, namespace: str) -> str:
        return os.path.join(self.root, f"{namespace}.json")

    def _load_file(self, namespace: str) -> dict:
        path = self._path(namespace)
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_file(self, namespace: str, data: dict) -> None:
        with open(self._path(namespace), "w", encoding="utf-8") as f:
            json.dump(data, f)

    def load(self, namespace: str, key: str):
        data = self._load_file(namespace)
        if key not in data:
            raise KeyError(key)
        return data[key]

    def store(self, namespace: str, key: str, data, overwrite: bool) -> None:
        file_data = self._load_file(namespace)
        if key in file_data and not overwrite:
            raise KeyError(key)
        file_data[key] = data
        self._write_file(namespace, file_data)

    def exists(self, namespace: str, key: str) -> bool:
        data = self._load_file(namespace)
        return key in data

    def delete(self, namespace: str, key: str) -> None:
        file_data = self._load_file(namespace)
        if key not in file_data:
            raise KeyError(key)
        del file_data[key]
        self._write_file(namespace, file_data)
