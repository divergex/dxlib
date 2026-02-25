import hashlib
import pickle
from typing import Callable, Any, Type, TypeVar

from .storable import Storable
from .storage import Storage


T = TypeVar("T")


class Cache(Storage):
    """
    Cache-like storage, s.t. data is stored relative to function/method calls instead of
    directly through the Storage API.
    """
    @staticmethod
    def _hash(func_name: str, *args, **kwargs):
        return hashlib.sha256(pickle.dumps((func_name, args, kwargs))).hexdigest()

    def exists(self,
               storage: str,
               func: Callable,
               *args,
               hash_function: Callable[[Callable, Any], str] = None,
               **kwargs,
               ):
        func_name = func.__name__
        key = hash_function(func_name, *args, **kwargs) if hash_function else self._hash(func_name, *args, **kwargs)

        return True

    def cache(self,
               namespace: str,
               expected_type: Type[Storable],
               func: Callable[..., T],
               *args,
               hash_function: Callable = None,
               **kwargs
               ) -> T:
        func_name = func.__qualname__
        key = hash_function(func_name, *args, **kwargs) if hash_function else self._hash(func_name, *args, **kwargs)

        try:
            return self.load(namespace, key, expected_type)
        except (ValueError, FileNotFoundError):
            obj = func(*args, **kwargs)
            self.store(namespace, key, obj, overwrite=True)  # None.
            return obj

    # cache decorator
    def cached(self,
              storage: str,
              expected_type: Type[T],
              hash_function: Callable = None,
              ) -> Callable[[Callable[..., T]], Callable[..., T]]:
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            def wrapper(*args, **kwargs) -> T:
                return self.cache(
                    storage,
                    expected_type,
                    func,
                    *args,
                    hash_function=hash_function,
                    **kwargs
                )
            return wrapper
        return decorator
