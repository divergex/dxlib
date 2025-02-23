from uuid import UUID


class Transaction:
    def __init__(self, seller: UUID, buyer: UUID, price, quantity):
        self.seller = seller
        self.buyer = buyer
        self.price = price
        self.quantity = quantity
