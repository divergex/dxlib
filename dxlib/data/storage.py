import hashlib
import os
import pickle
from typing import TypeVar, Type, Any

import h5py
import pandas as pd

from dxlib.data import Serializable


class Storage:
    """
    Cache class to manage HDF5 storage for objects.
    This class provides methods to store, extend, load, and verify existence of HDF5 caches.
    It does not depend on specific index names or columns, allowing flexibility for different history objects.
    """
    T = TypeVar('T')

    def __init__(self, cache_dir: str = None):
        """
        Initialize the Cache instance with a directory for storing the HDF5 files.

        Args:
            cache_dir (str): Directory where HDF5 files will be stored. Defaults to '{os.getcwd()}/.divergex'.
        """
        if cache_dir is None:
            cache_dir = os.path.join(os.getcwd(), '.divergex')
        self.cache_dir = cache_dir
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def _path(self, storage: str) -> str:
        """
        Generate the file path for the cache file of a given history object.

        Args:
            storage (str): The name/identifier for the storage unit.
        Returns:
            str: Full path to the cache file.
        """
        return os.path.join(self.cache_dir, f"{storage}.h5")

    # region Manipulation

    def load(self, storage: str, key: str, obj_type: Type[Serializable]) -> Serializable | None:
        """
        Load an object's data from an HDF5 cache file.

        Args:
            storage (str): The name/identifier for the storage unit.
            key (str): The key to load the data under in the storage unit.
            obj_type (Type[Serializable]): The object type to load.
        Returns:
            pd.DataFrame: The loaded data.

        Raises:
            KeyError: If the key is not present in the storage unit.
        """
        cache_path = self._path(storage)

        with h5py.File(cache_path, 'r') as f:
            data = f.get(key)[()].decode('utf-8')
            if obj_type:
                return obj_type.model_validate_json(data)
            return data

    def store(self, storage: str, key: str, data: Serializable, overwrite: bool = False):
        """
        Store an object's data in an HDF5 cache file.

        Args:
            storage (str): The name/identifier for the storage unit.
            key (str): The key to store the data under in the storage unit.
            data (Serializable): The object to store.
            overwrite (bool): If True, overwrite existing data.
        """
        cache_path = self._path(storage)

        if not overwrite:
            with h5py.File(cache_path, 'r') as f:
                if key in f:
                    raise KeyError("Key already exists. Use overwrite=True to overwrite.")

        with h5py.File(cache_path, 'w') as f:
            f.create_dataset(key, data=data.model_dump_json())

    # Cache a function call given its arguments, if the cache does not exist, else load it
    @staticmethod
    def _hash(*args, **kwargs):
        """
        Default hash function to generate a unique key for the cache.

        Args:
            *args: Positional arguments for the function.
            **kwargs: Keyword arguments for the function.
        Returns:
            str: The generated hash key.
        """
        # try to serialize with Registry, then with json
        data = pickle.dumps((args, kwargs))
        return hashlib.sha256(data).hexdigest()

    def cached(self,
               storage: str,
               func: callable,
               expected_type: Type[Serializable],
               *args,
               hash_function: callable = None,
               **kwargs) -> Any:
        method = func.__qualname__

        key = hash_function(method, *args, **kwargs) if hash_function else self._hash(method, *args, **kwargs)

        try:
            return self.load(storage, key, expected_type)
        except (KeyError, FileNotFoundError):
            data = func(*args, **kwargs)
            self.store(storage, key, data, overwrite=True)  # None.
            return data

    # endregion
