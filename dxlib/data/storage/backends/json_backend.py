import os
import json

from ..stored_field import StoredField, FieldFormat
from ..backend import StorageBackend
from ...registry import Registry


def _ensure_namespace(namespace: str):
    os.makedirs(namespace, exist_ok=True)

def _filename(namespace: str, key: str) -> str:
    # ensure namespace exists
    return os.path.join(namespace, f"{key}.json")

class JSONBackend(StorageBackend):
    def exists(self, namespace: str, key: str) -> bool:
        pass

    def delete(self, namespace: str, key: str) -> None:
        pass

    @property
    def supported_formats(self) -> set[FieldFormat]:
        return {FieldFormat.JSON, FieldFormat.SERIALIZABLE}

    def __init__(self, root):
        super().__init__(root)

    def store(self, namespace: str, key: str, data, overwrite: bool) -> None:
        _ensure_namespace(namespace)

        if Registry.is_serializable(data):
            data = Registry.serialize(data)

        with open(_filename(namespace, key), "w", encoding="utf-8") as file:
            json.dump(data, file)

    def load(self, namespace: str, key: str, cls) -> bytes:
        """
        Raises:
            ValueError: if key is empty or did not return a valid JSON.
        """
        filename = _filename(namespace, key)
        file = open(filename, "r")
        data = json.load(file)

        if cls is not None and Registry.is_serializable(cls):
            return Registry.deserialize(data, cls)
        return data
