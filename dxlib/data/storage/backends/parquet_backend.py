import os
import pandas as pd

from ..backend import StorageBackend


class ParquetBackend(StorageBackend):
    def __init__(self, root: str):
        super().__init__()
        self.root = root
        os.makedirs(root, exist_ok=True)

    def _dir(self, namespace: str) -> str:
        return os.path.join(self.root, namespace)

    def _path(self, namespace: str, key: str) -> str:
        return os.path.join(self._dir(namespace), f"{key}.parquet")

    def load(self, namespace: str, key: str, cls: type=None) -> bytes:
        path = self._path(namespace, key)
        df = pd.read_parquet(path)
        return df.iloc[0]["payload"]

    def store(self, namespace: str, key: str, data: bytes, overwrite: bool) -> None:
        os.makedirs(self._dir(namespace), exist_ok=True)
        path = self._path(namespace, key)

        if os.path.exists(path) and not overwrite:
            raise KeyError(key)

        df = pd.DataFrame({"payload": [data]})
        df.to_parquet(path, index=False)

    def exists(self, namespace: str, key: str) -> bool:
        return os.path.exists(self._path(namespace, key))

    def delete(self, namespace: str, key: str) -> None:
        os.remove(self._path(namespace, key))
