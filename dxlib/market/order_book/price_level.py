from llist import dllist


class PriceLevel:
    def __init__(self, price):
        self.price = price
        # double linked list
        self.orders = dllist()
        # hash map/dict
        self.order_map = {}

    def add_order(self, order):
        node = self.orders.append(order)
        self.order_map[order.uuid] = node

    def top(self):
        return self.orders.first.value

    def pop(self, order):
        node = self.order_map.pop(order.uuid)
        self.orders.remove(node)

    def __getitem__(self, idx):
        return self.orders[idx]

    def __len__(self):
        return len(self.orders)

    def __iter__(self):
        return iter(self.orders)

    def empty(self):
        return len(self.orders) == 0