from .size import Size
from .order import Order, Side


class MarketOrderFactory:
    @classmethod
    def percent_of_equity(cls, security, percent: float, side: Side) -> Order:
        return Order(security, price=None, quantity=Size(percent, 'percent_of_equity'), side=side)

    @classmethod
    def quantity(cls, security, quantity: float, side: Side) -> Order:
        return Order(security, price=None, quantity=quantity, side=side)
