from uuid import uuid4


class Order:
    def __init__(self, price, quantity, side, uuid=None):
        self.uuid = uuid4() if uuid is None else uuid
        self.price = price
        self.quantity = quantity
        self.side = side

