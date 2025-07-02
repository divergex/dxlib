from enum import Enum
from uuid import uuid4

from dxlib.core import Security
from dxlib.types import TypeRegistry


class Side(Enum):
    NONE = 0
    BUY = 1
    SELL = -1

    @property
    def value(self) -> int:
        return super(Side, self).value


class Order(TypeRegistry):
    def __init__(self, security, price, quantity, side: Side, uuid=None, client=None):
        self.security: Security = security
        self.uuid = uuid4() if uuid is None else uuid
        self.price = price
        self.quantity = quantity
        self.side = side
        self.client = None

    def value(self):
        return self.side.value * self.price * self.quantity

    def __str__(self):
        return f"Order({self.security}, {self.price}, {self.quantity}, {self.side})"

    @classmethod
    def none(cls):
        return Order(
            security=None,
            price=None,
            quantity=None,
            side=Side.NONE,
        )

    def is_none(self):
        return self.security is None and self.side == Side.NONE
