import os
from typing import Set

from .backend import StorageBackend
from .backends import HDF5Backend, JSONBackend
from .storable import Storable
from .stored_field import StoredField, FieldFormat


class Storage:
    backends: Set[StorageBackend]

    def __init__(self, root_path=".dx", backends=None):
        if backends is None:
            backends = {JSONBackend(root_path), HDF5Backend(root_path)}

        self.meta_backend = JSONBackend(root_path)
        self.backends = backends
        self.root_path = root_path

    def _path(self, namespace: str, key: str) -> str:
        return os.path.join(self.root_path, namespace, key)

    def register_backend(self, backend: StorageBackend):
        self.backends.add(backend)

    def backend_for(self, field_format: FieldFormat) -> StorageBackend:
        for backend in self.backends:
            if backend.supports(field_format) or field_format == FieldFormat.ANY:
                return backend
        raise RuntimeError(f"No backend for format {field_format}")

    def store(self, namespace: str, key: str, obj: Storable, overwrite: bool) -> None:
        local_namespace = self._path(namespace, key)
        manifest = {}
        for field_name, field in obj.stored_fields().items():
            data = getattr(obj, field_name)
            manifest[field_name] = type(data).__name__
            backend = self.backend_for(field.field_format)
            backend.store(local_namespace, key=field_name, data=data, overwrite=overwrite)
        self.meta_backend.store(local_namespace, key="manifest", data=manifest, overwrite=True)

    def load(self, namespace, key, cls: type[Storable]) -> Storable:
        local_namespace = self._path(namespace, key)
        obj = cls.__new__(cls)
        for field_name, field in cls.stored_fields().items():
            backend = self.backend_for(field.field_format)
            data = backend.load(local_namespace, field_name, field.field_type)
            setattr(obj, field_name, data)
        return obj
