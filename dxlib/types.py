from datetime import datetime
from numbers import Number
from typing import Dict, Type

from pandas import Timestamp

_TYPES: Dict[str, Type] = {str(t): t for t in [int, float, str, bool, datetime, Timestamp, Number]}

class TypeRegistry:
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        _TYPES[str(cls)] = cls

    @classmethod
    def get(cls, name):
        return _TYPES[name]

    @classmethod
    def register(cls, obj):
        _TYPES[str(obj)] = obj
