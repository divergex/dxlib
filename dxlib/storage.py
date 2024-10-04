import datetime
import functools
import json
from abc import ABC, ABCMeta, abstractmethod
from typing import TypeVar, Type

import numpy as np
import pandas as pd
import os


class RegistryBase(ABCMeta):
    """
    Class for dealing with common registries. Useful for HTTP, file and database serialization.

    An example use case is to register numpy types, such as np.int64, np.float64, etc that would be used
    in a HistorySchema instance, and then register their class names.

    A deserializer can then use the registry to map the class names to the actual types.
    """

    # default classes: int, float, np.float64, np.int64, str, bool, datetime.datetime
    REGISTRY = {
        'int': int,
        'float': float,
        'float64': np.float64,
        'int64': np.int64,
        'str': str,
        'bool': bool,
        'datetime.datetime': datetime.datetime,
        'Timestamp': pd.Timestamp
    }

    def __new__(cls, *args, **kwargs):
        new_cls = super().__new__(cls, *args, **kwargs)
        cls.REGISTRY[new_cls.__name__] = new_cls
        return new_cls

    @classmethod
    def get(cls, name: str):
        return cls.REGISTRY[name]

    @classmethod
    def register(cls, obj, custom_name: str = None):
        cls.REGISTRY[obj.__name__ if custom_name is None else custom_name] = obj


class Serializable(ABC):
    """
    Base class for all objects serializable. Useful for annotating network-related or disk-related objects.
    """
    T = TypeVar('T')

    @abstractmethod
    def to_dict(self) -> dict:
        """
        Serialize the object to a JSON-compatible dictionary.

        Returns:
            dict: The serialized object.
        """
        pass

    @classmethod
    @abstractmethod
    def from_dict(cls: Type[T], data: dict) -> T:
        """
        Deserialize the object from a JSON-compatible dictionary.

        Args:
            data (dict): The serialized object data.
        """
        pass

    def __json__(self):
        return json.dumps(self.to_dict())


class Cache:
    """
    Cache class to manage HDF5 storage for History objects.
    This class provides methods to store, extend, load, and verify existence of HDF5 caches.
    It does not depend on specific index names or columns, allowing flexibility for different history objects.
    """

    def __init__(self, cache_dir: str = None):
        """
        Initialize the Cache instance with a directory for storing the HDF5 files.

        Args:
            cache_dir (str): Directory where HDF5 files will be stored. Defaults to './cache'.
        """
        if cache_dir is None:
            cache_dir = os.path.join(os.getcwd(), '.cache/')
        self.cache_dir = cache_dir
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    # region Manipulation

    def _get_cache_path(self, history_name: str) -> str:
        """
        Generate the file path for the cache file of a given history object.

        Args:
            history_name (str): The unique name of the history object (can be "symbol" or other identifier).

        Returns:
            str: Full path to the cache file.
        """
        return os.path.join(self.cache_dir, f"{history_name}.h5")

    def remove_cache(self):
        """
        Remove the cache directory and all its contents.
        """
        if os.path.exists(self.cache_dir):
            # remove all files in the cache directory
            for file in os.listdir(self.cache_dir):
                os.remove(os.path.join(self.cache_dir, file))
            # remove the cache directory
            os.rmdir(self.cache_dir)

    def store(self, history_name: str, data: pd.DataFrame):
        """
        Store a history object's data in an HDF5 cache file.

        Args:
            history_name (str): The name/identifier for the history object (e.g., symbol).
            data (pd.DataFrame): The DataFrame containing the history data.
        """
        cache_path = self._get_cache_path(history_name)
        data.to_hdf(cache_path, key='history', mode='w', format='table')
        print(f"Stored cache for '{history_name}' at '{cache_path}'")

    def extend(self, history_name: str, new_data: pd.DataFrame):
        """
        Extend an existing HDF5 cache with new data.

        Args:
            history_name (str): The name/identifier for the history object.
            new_data (pd.DataFrame): The new data to append to the existing cache.
        """
        cache_path = self._get_cache_path(history_name)
        if not os.path.exists(cache_path):
            raise ValueError(f"No cache exists for '{history_name}'. Cannot extend a non-existing cache.")

        # Load existing data
        existing_data = self.load(history_name)

        # Combine and remove duplicates
        combined_data = pd.concat([existing_data, new_data]).drop_duplicates()

        # Overwrite the cache with the combined data
        combined_data.to_hdf(cache_path, key='history', mode='w', format='table')
        print(f"Extended cache for '{history_name}' at '{cache_path}'")

    def load(self, history_name: str) -> pd.DataFrame:
        """
        Load a history object's data from an HDF5 cache file.

        Args:
            history_name (str): The name/identifier for the history object.

        Returns:
            pd.DataFrame: The loaded history data.
        """
        cache_path = self._get_cache_path(history_name)
        if not os.path.exists(cache_path):
            raise FileNotFoundError(f"No cache found for '{history_name}'.")

        # Load the data from the HDF5 cache
        data = pd.read_hdf(cache_path, key='history')
        print(f"Loaded cache for '{history_name}' from '{cache_path}'")

        # pytables to pandas
        return pd.DataFrame(data)

    def exists(self, history_name: str) -> bool:
        """
        Check if a cache file exists for a given history object.

        Args:
            history_name (str): The name/identifier for the history object.

        Returns:
            bool: True if the cache exists, False otherwise.
        """
        cache_path = self._get_cache_path(history_name)
        return os.path.exists(cache_path)

    # endregion

    # region Method decorators

    def cache(self, key: str, extendable: bool = False):
        """Cache decorator to cache function results."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):

                exists = self.exists(key)

                if exists:
                    return self.load(key)

                result = func(*args, **kwargs)

                if exists and extendable:
                    self.extend(key, result)
                elif not exists:
                    self.store(key, result)

                return result

            return wrapper

        return decorator

    # endregion
