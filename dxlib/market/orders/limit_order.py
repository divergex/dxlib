from .size import Size
from .order import Order, Side


class LimitOrderFactory:
    @classmethod
    def create(cls, security, price, quantity, side: Side):
        return Order(security, security, price, quantity, side)

    @classmethod
    def percent_of_equity(cls, security, price, percent, side: Side) -> Order:
        return Order(security, price, quantity=Size(percent, 'percent_of_equity'), side=side)
