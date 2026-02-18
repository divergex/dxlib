import os
import h5py

from ..stored_attribute import AttributeFormat
from ..backend import StorageBackend


class HDF5Backend(StorageBackend):
    @property
    def supported_formats(self) -> set[AttributeFormat]:
        return {AttributeFormat.DATAFRAME}

    def __init__(self, root: str):
        super().__init__()
        self.root = root
        os.makedirs(root, exist_ok=True)

    def _path(self, namespace: str) -> str:
        return os.path.join(self.root, f"{namespace}.h5")

    def load(self, namespace: str, key: str) -> bytes:
        with h5py.File(self._path(namespace), "r") as f:
            return f[key][()]

    def store(self, namespace: str, key: str, data: bytes, overwrite: bool) -> None:
        mode = "a"
        with h5py.File(self._path(namespace), mode) as f:
            if key in f:
                if not overwrite:
                    raise KeyError(key)
                del f[key]
            f.create_dataset(key, data=data)

    def exists(self, namespace: str, key: str) -> bool:
        try:
            with h5py.File(self._path(namespace), "r") as f:
                return key in f
        except OSError:
            return False

    def delete(self, namespace: str, key: str) -> None:
        with h5py.File(self._path(namespace), "a") as f:
            del f[key]
