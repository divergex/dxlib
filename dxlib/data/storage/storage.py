import os
from typing import Set

from .backend import StorageBackend
from .backends import HDF5Backend, JSONBackend
from .storable import Storable
from .stored_attribute import AttributeFormat


class Storage:
    backends: Set[StorageBackend]

    def __init__(self, root_path=".dx", backends=None):
        if backends is None:
            backends = {JSONBackend(root_path), HDF5Backend(root_path)}

        self.backends = backends
        self.root_path = root_path

    def path(self, storage: str) -> str:
        return os.path.join(self.root_path, storage)

    def register_backend(self, backend: StorageBackend):
        self.backends.add(backend)

    def backend_for(self, attribute_format: AttributeFormat) -> StorageBackend:
        for backend in self.backends:
            if backend.supports(attribute_format) or attribute_format == AttributeFormat.ANY:
                return backend
        raise RuntimeError(f"No backend for format {attribute_format}")

    def store(self, namespace: str, key: str, obj: Storable, overwrite: bool):
        for name, field in obj.fields().items():
            backend = self.backend_for(field.attribute)
            backend.store(namespace, f"{key}.{name}", getattr(obj, name), overwrite)

    def load(self, namespace: str, key: str, cls: type[Storable]):
        obj = cls.__new__(cls)
        for name, field in cls.fields().items():
            backend = self.backend_for(field.attribute)
            value = backend.load(namespace, f"{key}.{name}")
            setattr(obj, name, value)
        return obj
