import inspect
from typing import Any, Callable, Dict, List, Tuple, Union
from functools import wraps


class CallbackBase:
    def __init__(self):
        self._callbacks: Dict[int, Callable[[Any], None]] = {}

    # Callback handling
    def set_callback(self, identifier: Any, callback: Callable[[Any], None]):
        self._callbacks[identifier] = callback

    def call_callback(self, identifier: Any, *args, **kwargs):
        if identifier in self._callbacks:
            self._callbacks[identifier](*args, **kwargs)

    # Decorators
    @staticmethod
    def callback(*arg_names: Union[str, Tuple[str]]):
        """Decorator for callback methods. Pass argument names for the identifier."""

        def decorator(func: Callable):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                identifier = tuple(kwargs[arg] if arg in kwargs else args[idx]
                                   for idx, arg in enumerate(arg_names))
                identifier = identifier[0] if len(identifier) == 1 else identifier

                self.call_callback(identifier, *args, **kwargs)
                return func(self, *args, **kwargs)

            return wrapper

        return decorator


class StoreBase:
    def __init__(self):
        self._data_storage: Dict[Any, List[Any]] = {}
        self._data_end: Dict[Any, bool] = {}

    # Data storage handling
    def store_data(self, identifier: Any, data: Any):
        self._data_storage.setdefault(identifier, []).append(data)

    def get_data(self, identifier: Any) -> List[Any] | None:
        return self._data_storage.get(identifier, None).pop(0)

    def set_end(self, identifier: Any):
        self._data_end[identifier] = True

    def get_end(self, identifier: Any) -> bool:
        return self._data_end.get(identifier, False)

    def clear_data(self, identifier: Any):
        if identifier in self._data_storage:
            del self._data_storage[identifier]
        if identifier in self._data_end:
            del self._data_end[identifier]

    def store(*args: str, keys: tuple = ()):
        def decorator(func: Callable):
            def wrapper(self, *func_args, **func_kwargs):
                data = func(self, *func_args, **func_kwargs)
                identifier = args[0] if len(args) == 1 else args

                if keys:
                    data = {key: data[idx] for idx, key in enumerate(keys)}

                self.store_data(identifier, data)
                return data
            return wrapper
        return decorator