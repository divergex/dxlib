from abc import ABC, abstractmethod
from dataclasses import dataclass

from .stored_field import FieldFormat


@dataclass
class StorageCapabilities:
    mutable: bool
    append_only: bool
    concurrent_read: bool
    concurrent_write: bool


class StorageBackend(ABC):
    def __init__(self, root):
        self.capabilities = StorageCapabilities(True, False, True, True)

    @property
    @abstractmethod
    def supported_formats(self) -> set[FieldFormat]:
        ...

    def supports(self, field_format: FieldFormat) -> bool:
        return field_format in self.supported_formats


    @abstractmethod
    def load(self, namespace: str, key: str, cls) -> bytes:
        ...

    @abstractmethod
    def store(self, namespace: str, key: str, data, overwrite: bool) -> None:
        ...

    @abstractmethod
    def exists(self, namespace: str, key: str) -> bool:
        ...

    @abstractmethod
    def delete(self, namespace: str, key: str) -> None:
        ...

