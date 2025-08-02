from enum import Enum

from dxlib.types import TypeRegistry
from .orders import Side


class Signal(TypeRegistry, Enum):
    BUY = 1
    SELL = -1
    HOLD = 0

    def to_side(self):
        return Side(self.value)

    def has_side(self) -> bool:
        return self is not Signal.HOLD
