from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Union

from .signal import Side, Signal
from .transaction import Transaction
from ..components.security import Security


class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"

    def __repr__(self):
        return self.name

    def to_json(self):
        return self.value


@dataclass
class OrderData:
    def __init__(
            self,
            security: Security,
            price: float | int = None,
            quantity: float | int = 0,
            side: Side | int = Side.BUY,
            order_type: OrderType = OrderType.MARKET,
    ):
        self.security = security
        self.price = price
        self.quantity = quantity
        self.side: Side = Side(side)
        self.order_type = order_type

    def __repr__(self):
        return (
            f"OrderDetails({self.side}: {self.security} {self.quantity} @ {self.price})"
        )

    def __str__(self):
        return f"{self.side.name}: {self.security} {self.quantity} @ {self.price}"

    def to_dict(self):
        return {
            "security": self.security,
            "price": self.price,
            "quantity": self.quantity,
            "side": self.side.to_dict(),
            "order_type": self.order_type.to_json(),
        }

    @classmethod
    def from_dict(cls, **kwargs):
        return cls(
            security=Security.from_dict(
                **kwargs.get("security")
            ),
            price=kwargs.get("price"),
            quantity=kwargs.get("quantity"),
            side=Side.from_dict(
                **kwargs.get("side")
            ),
            order_type=OrderType(kwargs.get("order_type"))
        )

    @classmethod
    def from_signal(cls, signal: Signal, security: Security):
        return cls(
            security=security,
            price=signal.price,
            quantity=signal.quantity,
            side=signal.side
        )


class Order:
    def __init__(
            self,
            data: Union[OrderData, dict],
            transactions: list[Transaction] = None,
    ):
        self._data = data
        self._transactions: list[Transaction] = transactions or []

    @property
    def data(self):
        return self._data

    def __repr__(self):
        return f"Order({self.data.__repr__()}, [{len(self._transactions)}])"

    def __str__(self):
        return f"{self.data.__str__()} (#{len(self._transactions)} transactions)"

    def __getitem__(self, item):
        return self._transactions[item]

    def __len__(self):
        return len(self._transactions)

    def __iter__(self):
        return iter(self._transactions)

    def to_dict(self):
        return {
            "data": self._data.to_dict(),
            "transactions": [t.to_dict() for t in self._transactions],
        }

    def add_transaction(self, transaction: Transaction | list[Transaction]):
        if transaction.security != self._data.security:
            raise ValueError("Transaction security must match order security")
        if isinstance(transaction, list):
            self._transactions.extend(transaction)
        elif isinstance(transaction, Transaction):
            self._transactions.append(transaction)

    @property
    def executed_quantity(self):
        return sum([t.quantity for t in self._transactions])

    @property
    def security(self):
        return self._data.security

    @property
    def price(self):
        return self._data.price

    @property
    def quantity(self):
        return self._data.quantity

    @quantity.setter
    def quantity(self, quantity):
        self._data.quantity = quantity

    @property
    def remaining_quantity(self):
        return self._data.quantity - self.executed_quantity

    @property
    def side(self):
        return self._data.side

    @property
    def order_type(self):
        return self._data.order_type

    @classmethod
    def from_data(cls,
                  security: Security,
                  price: float | int = None,
                  quantity: float | int = 0,
                  side: Side | int = Side.BUY,
                  order_type: OrderType = OrderType.MARKET
                  ):
        return cls(OrderData(security, price, quantity, side, order_type))
