import hashlib
import os
import pickle
import tempfile
from typing import TypeVar, Type, Callable, Any

import h5py
import pandas as pd

from .registry import Registry
from .serializable import Serializable


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
        # ensure path exists
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)

        with h5py.File(cache_path, 'r') as f:
            obj_data = f.get(key)
            if not obj_data:
                raise KeyError(f"Key {key} not found in {cache_path}")
            data = obj_data[()].decode('utf-8')

            return obj_type.model_validate_json(data) if obj_type is not None else data

    def is_writable(self):
        try:
            testfile = tempfile.TemporaryFile(dir=self.cache_dir)
            testfile.close()
            return True
        except (OSError, PermissionError):
            return False

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

    def exists(self,
               storage: str,
               func: Callable,
               *args,
               hash_function: Callable[[str, Any], str] = None,
               **kwargs,
               ):
        func_name = func.__qualname__
        key = hash_function(func_name, *args, **kwargs) if hash_function else self._hash(func_name, *args, **kwargs)
        cache_path = self._path(storage)

        try:
            with h5py.File(cache_path, 'r') as f:
                if key in f:
                    return True
            return False
        except (OSError, KeyError):
            return False


    def cached(self,
               storage: str,
               expected_type: Type[T],
               func: callable,
               *args,
               hash_function: Callable = None,
               **kwargs
               ) -> T:
        model = Registry.get(expected_type)

        func_name = func.__qualname__
        key = hash_function(func_name, *args, **kwargs) if hash_function else self._hash(func_name, *args, **kwargs)

        try:
            return self.load(storage, key, model).to_domain()
        except (KeyError, FileNotFoundError):
            obj = func(*args, **kwargs)
            if self.is_writable():
                self.store(storage, key, model.from_domain(obj), overwrite=True)  # None.
            return obj

    # endregion
