import contextlib
import datetime
import functools
import json
from abc import ABC, ABCMeta, abstractmethod
from typing import TypeVar, Type, Generator

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
        'datetime': datetime.datetime,
        'Timestamp': pd.Timestamp
    }

    def __new__(cls, name, bases, attrs):
        new_cls = super().__new__(cls, name, bases, attrs)
        cls.REGISTRY[new_cls.__name__] = new_cls
        return new_cls

    @classmethod
    def reg(cls, obj, custom_name: str = None):
        cls.REGISTRY[obj.__name__ if custom_name is None else custom_name] = obj

T = TypeVar('T')

class Serializable(ABC):
    """
    Base class for all objects serializable. Useful for annotating network-related or disk-related objects.
    """

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

    def store(self, cache_path, key):
        raise NotImplementedError

    @classmethod
    def load(cls, storage_path, key):
        raise NotImplementedError

    @classmethod
    def cache_exists(cls, storage_path, key):
        raise NotImplementedError


class Cache:
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
            cache_dir (str): Directory where HDF5 files will be stored. Defaults to './cache'.
        """
        if cache_dir is None:
            cache_dir = os.path.join(os.getcwd(), '.cache')
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
        return os.path.join(self.cache_dir, f"{storage}")

    def exists(self, storage: str, key: str, object_type: Type[Serializable]) -> bool:
        """
        Check if a cache file exists for a given history object.

        Args:
            storage (str): The name/identifier for the storage unit.
            key (str): The key to check for in the storage unit.
            object_type (Serializable): The type of object to check for.
        Returns:
            bool: True if the cache exists, False otherwise.
        """
        storage_path = self._path(storage)
        try:
            assert os.path.exists(storage_path)
        except AssertionError:
            return False
        with contextlib.suppress(FileNotFoundError):
            return object_type.cache_exists(storage_path, key)


    # region Manipulation

    def load(self, storage: str, key: str) -> (str, str):
        """
        Load an object's data from an HDF5 cache file.

        Args:
            storage (str): The name/identifier for the storage unit.
            key (str): The key to load the data under in the storage unit.
        Returns:
            pd.DataFrame: The loaded data.
        """
        cache_path = self._path(storage)
        if not os.path.exists(cache_path):
            os.makedirs(cache_path)

        return cache_path, key

    # endregion

