from abc import ABC, abstractmethod
from dataclasses import dataclass

from .stored_attribute import AttributeFormat


@dataclass
class StorageCapabilities:
    mutable: bool
    append_only: bool
    concurrent_read: bool
    concurrent_write: bool


class StorageBackend(ABC):
    def __init__(self):
        self.capabilities = StorageCapabilities(True, False, True, True)

    @property
    @abstractmethod
    def supported_formats(self) -> set[AttributeFormat]:
        ...

    def supports(self, attribute_format: AttributeFormat) -> bool:
        return attribute_format in self.supported_formats

    @abstractmethod
    def load(self, namespace: str, key: str) -> bytes:
        ...

    @abstractmethod
    def store(self, namespace: str, key: str, data: bytes, overwrite: bool) -> None:
        ...

    @abstractmethod
    def exists(self, namespace: str, key: str) -> bool:
        ...

    @abstractmethod
    def delete(self, namespace: str, key: str) -> None:
        ...
