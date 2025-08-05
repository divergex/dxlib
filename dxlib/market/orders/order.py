import numbers
from enum import Enum
from uuid import uuid4

import numpy as np

from dxlib.core import Instrument, Signal
from dxlib.types import TypeRegistry


class Side(Enum):
    NONE = 0
    BUY = 1
    SELL = -1

    @property
    def value(self) -> int:
        return super(Side, self).value

    @classmethod
    def signed(cls, x: numbers.Number | np.number):
        return cls((float(x) > 0) - (float(x) < 0))
    
    @classmethod
    def from_signal(cls, signal: Signal):
        return cls(signal.value)

class Order(TypeRegistry):
    def __init__(self, instrument, price, quantity, side: Side, uuid=None, client=None):
        self.instrument: Instrument = instrument
        self.uuid = uuid4() if uuid is None else uuid
        self.price = price
        self.quantity = quantity
        self.side = side
        self.client = None

    def value(self):
        return self.side.value * self.price * self.quantity

    def __str__(self):
        return f"Order({self.instrument}, {self.price}, {self.quantity}, {self.side})"

    def __repr__(self):
        return f"Order({self.instrument}, {self.price}, {self.quantity}, {self.side})"

    @classmethod
    def none(cls):
        return Order(
            instrument=None,
            price=None,
            quantity=None,
            side=Side.NONE,
        )

    def is_none(self):
        return self.instrument is None and self.side == Side.NONE
