from collections import deque
from typing import Dict
from uuid import UUID

from .orders.order import Order, Side
from .transaction import Transaction
from .red_black_tree import RedBlackTree


class PriceLevel:
    def __init__(self, price):
        self.price = price
        self.orders = deque()

    def add_order(self, order):
        self.orders.append(order)

    def remove_order(self):
        return self.orders.popleft()

    def head(self):
        return self.orders[0]


class OrderBook:
    def __init__(self, tick_size=2):
        self.asks = RedBlackTree()
        self.bids = RedBlackTree()
        self.orders: Dict[UUID, Order] = {}
        self.tick_size = tick_size

    def clear(self):
        self.asks.clear()
        self.bids.clear()
        self.orders = {}

    def cancel_order(self, order_id):
        if not order_id in self.orders:
            return
        order = self.orders[order_id]
        tree = self.asks if order.side == 'ask' else self.bids
        level = tree.search(order.price)
        if level is not None:
            level.value.orders = deque(filter(lambda x: x.uuid != order_id, level.value.orders))
            if not level.value.orders:
                tree.delete(order.price)
        del self.orders[order_id]

    def quantity(self, price: float, side: Side):
        price = round(price, self.tick_size)
        price_level = PriceLevel(price)
        return self._quantity(price_level, side)

    def queue_ahead(self, price: float, side: Side):
        price = round(price, self.tick_size)
        if side == Side.BUY:
            tree = self.bids
            get_best = tree.top
            compare = lambda x, y: x > y
        elif side == Side.SELL:
            tree = self.asks
            get_best = tree.bottom
            compare = lambda x, y: x < y
        else:
            raise ValueError("side must be Side.BUY or Side.SELL")

        total_quantity = 0
        best_level = get_best()
        best_level = best_level.value if best_level is not None else None

        while best_level is not None and compare(best_level.price, price):
            total_quantity += sum(order.quantity for order in best_level.orders)
            next_node = tree.successor(best_level.price)
            best_level = next_node.value if next_node is not None else None

        return total_quantity

    def _quantity(self, price_level: PriceLevel, side: Side):
        # return accumulated quantity at a given price level
        if side == Side.BUY:
            tree = self.bids
        elif side == Side.SELL:
            tree = self.asks
        else:
            raise ValueError("side must be Side.BUY or Side.SELL")

        level = tree.search(price_level.price)
        if level is None:
            return 0
        return sum(order.quantity for order in level.value.orders)


    @property
    def shape(self):
        return len(self.bids), len(self.asks)

    # noinspection D
    def send_order(self, order: Order):
        order.price = round(order.price, self.tick_size)
        tree = self.asks if order.side == Side.SELL else self.bids
        match_tree = self.bids if order.side == Side.SELL else self.asks

        transactions = []

        if order.side == Side.SELL:
            def make_transaction(sender, receiver, price, quantity):
                return Transaction(sender, receiver, price, quantity)
        else:
            def make_transaction(sender, receiver, price, quantity):
                return Transaction(receiver, sender, price, quantity)

        compare = (lambda x, y: x <= y) if order.side == Side.SELL else (lambda x, y: x >= y)

        get_best = self.bids.top if order.side == Side.SELL else self.asks.bottom
        best_level = get_best()
        best_level = best_level.value if best_level is not None else None

        while best_level is not None and compare(order.price, best_level.price) and order.quantity > 0:
            while best_level.orders and order.quantity > 0:
                best_order = best_level.head()
                if best_order.quantity <= order.quantity:
                    transactions.append(
                        make_transaction(order.client, best_order.client, best_order.price, best_order.quantity))
                    order.quantity -= best_order.quantity
                    best_level.remove_order()
                    self.orders[best_order.uuid].quantity = 0
                    best_order.quantity = 0
                else:
                    transactions.append(make_transaction(order.client, best_order.client, best_order.price, order.quantity))
                    best_order.quantity -= order.quantity
                    self.orders[best_order.uuid].quantity = best_order.quantity
                    order.quantity = 0

                if best_order.quantity == 0:
                    del self.orders[best_order.uuid]
                    match_tree.delete(best_order.price)
                    best_level = get_best()
                    if best_level is not None:
                        best_level = best_level.value
                    else:
                        break
                else:
                    break

            if best_level is not None and best_level.orders:
                break

            best_level = get_best().value if get_best() is not None else None

        if order.quantity > 0:
            if tree.search(order.price) is None:
                tree.insert(order.price, PriceLevel(order.price))
            tree.search(order.price).value.add_order(order)
            self.orders[order.uuid] = order

        return transactions

    def update_order(self, order):
        self.orders[order.uuid] = order

    def add_asks(self, asks):
        for price, quantity in asks:
            order = Order(instrument=None, price=price, quantity=quantity, side=Side.SELL)
            self.orders[order.uuid] = order
            self.asks.insert(order.price, PriceLevel(price))
            self.asks.search(order.price).value.add_order(order)

    def add_bids(self, bids):
        for price, quantity in bids:
            order = Order(instrument=None, price=price, quantity=quantity, side=Side.BUY)
            self.orders[order.uuid] = order
            self.bids.insert(order.price, PriceLevel(price))
            self.bids.search(order.price).value.add_order(order)

    def depth(self, n_levels=5, side=None):
        # returns the top n_levels of bids and asks as two lists of (price, total_quantity)
        tree = self.bids if side == Side.BUY else self.asks if side == Side.SELL else None
        if tree is None:
            raise ValueError("side must be Side.BUY or Side.SELL")
        for price_level in tree.ordered_traversal():
            if n_levels <= 0:
                break
            level = price_level.value
            total_quantity = sum(order.quantity for order in level.orders)
            yield level.price, total_quantity
            n_levels -= 1

    def plot(self, n_levels=5, fig=None, ax=None):
        """
        Plots the limit order book as an area under a line chart with bids and asks prices on the x-axis
        and accumulated quantities on the y-axis.
        """
        bid_prices = []
        bid_quantities = []
        cumulative_quantity = 0

        for node in self.bids.ordered_traversal(reverse=True):  # Reverse for descending prices
            level = node.value
            cumulative_quantity += sum(order.quantity for order in level.orders)
            bid_prices.append(level.price)
            bid_quantities.append(cumulative_quantity)

        ask_prices = []
        ask_quantities = []
        cumulative_quantity = 0

        for node in self.asks.ordered_traversal():  # Ascending order
            level = node.value
            cumulative_quantity += sum(order.quantity for order in level.orders)
            ask_prices.append(level.price)
            ask_quantities.append(cumulative_quantity)

        bid_prices = bid_prices[:n_levels]
        bid_quantities = bid_quantities[:n_levels]
        ask_prices = ask_prices[:n_levels]
        ask_quantities = ask_quantities[:n_levels]

        if fig is None or ax is None:
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots(figsize=(12, 6))

        ax.plot(bid_prices, bid_quantities, color="green", label="Bids")
        ax.fill_between(bid_prices, bid_quantities, color="green", alpha=0.3)

        ax.plot(ask_prices, ask_quantities, color="red", label="Asks")
        ax.fill_between(ask_prices, ask_quantities, color="red", alpha=0.3)

        ax.grid(True, linestyle="--", alpha=0.7)

        # plot midprice as a vertical line
        midprice = (ask_prices[0] + bid_prices[0]) / 2 if ask_prices and bid_prices else (
            ask_prices[0] if ask_prices else bid_prices[0])
        ax.axvline(x=midprice, color='black', linestyle='--', label='Midprice')

        ax.set_xlabel("Price")
        ax.set_ylabel("Accumulated Quantity")
        ax.set_title("Limit Order Book")
        ax.legend()

        return fig, ax
    