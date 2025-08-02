from uuid import UUID

from .orders import Order


class Transaction:
    def __init__(self, seller: UUID | str, buyer: UUID | str, price, quantity):
        self.seller = seller
        self.buyer = buyer
        self.price = price
        self.quantity = quantity


class OrderTransaction:
    def __init__(self, order: Order, price: float, quantity: float):
        self.order: Order = order
        self.price = price
        self.quantity = quantity

    @property
    def value(self):
        return self.amount * self.price

    @property
    def amount(self):
        return self.order.side.value * self.quantity

    def __str__(self):
        return f"OrderTransaction({self.order}, {self.price}, {self.quantity})"