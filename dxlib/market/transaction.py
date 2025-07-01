from uuid import UUID

from .orders import Order


class Transaction:
    def __init__(self, seller: UUID | str, buyer: UUID | str, price, quantity):
        self.seller = seller
        self.buyer = buyer
        self.price = price
        self.quantity = quantity


class OrderTransaction:
    def __init__(self, order: Order, price, quantity):
        self.order: Order = order
        self.price = price
        self.quantity = quantity

    def __getattr__(self, item):
        assert hasattr(self.order, item), "Object '%s' has no attribute '%s'" % (self.order.__class__.__name__, item)
        return getattr(self.order, item)

    @property
    def value(self):
        return self.amount * self.price

    @property
    def amount(self):
        return self.order.side.value * self.quantity
