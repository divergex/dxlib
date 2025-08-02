from .size import Size
from .order import Order, Side


class LimitOrderFactory:
    @classmethod
    def sized(cls, instrument, price, quantity, side: Side):
        return Order(instrument, price, quantity, side)

    @classmethod
    def bid(cls, instrument, price, quantity = 1):
        return cls.sized(instrument, price, quantity, Side.BUY)

    @classmethod
    def ask(cls, instrument, price, quantity = 1):
        return cls.sized(instrument, price, quantity, Side.SELL)

    @classmethod
    def percent_of_equity(cls, instrument, price, percent, side: Side) -> Order:
        return Order(instrument, price, quantity=Size(percent, 'percent_of_equity'), side=side)
