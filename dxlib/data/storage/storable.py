from typing import Dict

from .stored_attribute import StoredAttribute

_FIELD_LAYOUT_KEY = "_storable_field_layout"


class StorableMeta(type):
    def __new__(mcs, name, bases, namespace):
        field_layout = {}
        for base in bases:
            if not isinstance(base, Storable):
                continue
            layout = getattr(base, _FIELD_LAYOUT_KEY, None)
            field_layout.update(layout)

        for key, value in namespace.items():
            if isinstance(value, StoredAttribute):
                value.name = key
                field_layout[key] = value

        namespace[_FIELD_LAYOUT_KEY] = field_layout
        return super().__new__(mcs, name, bases, namespace)


class Storable(metaclass=StorableMeta):
    @classmethod
    def fields(cls) -> Dict[str, StoredAttribute]:
        return getattr(cls, _FIELD_LAYOUT_KEY, {})
