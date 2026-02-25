import os
import h5py
import pandas as pd

from ..stored_field import FieldFormat
from ..backend import StorageBackend


class HDF5Backend(StorageBackend):
    def delete(self, namespace: str, key: str) -> None:
        pass

    @property
    def supported_formats(self) -> set[FieldFormat]:
        return {FieldFormat.DATAFRAME}

    def __init__(self, root: str):
        super().__init__(root)
        self.root = root
        os.makedirs(root, exist_ok=True)

    def _filename(self, namespace: str) -> str:
        path = os.path.join(namespace, "dataframes.h5")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def load(self, namespace: str, key: str, cls: type=None) -> pd.DataFrame:
        return pd.read_hdf(self._filename(namespace), key=key)

    def store(self, namespace: str, key: str, data: pd.DataFrame, overwrite: bool) -> None:
        path = self._filename(namespace)
        if self.exists(namespace, key):
            if not overwrite:
                raise KeyError(key)
            with h5py.File(path, "a") as f:
                del f[key]
        data.to_hdf(path, key=key, mode="a")

    def exists(self, namespace: str, key: str) -> bool:
        try:
            with h5py.File(self._filename(namespace), "r") as f:
                return key in f
        except OSError:
            return False