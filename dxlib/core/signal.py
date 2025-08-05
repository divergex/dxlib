from enum import Enum

from dxlib.types import TypeRegistry


class Signal(TypeRegistry, Enum):
    HOLD = 0
    BUY = 1
    SELL = -1


    def has_side(self) -> bool:
        return self is not Signal.HOLD
