from typing import Dict
from uuid import UUID

from sortedcontainers import SortedDict

from ..transaction import Transaction
from ..orders.order import Order, Side

from .price_level import PriceLevel


class OrderBook:
    def __init__(self, tick_size: int | float = 1e-2):
        self.asks: Dict[float, PriceLevel] = SortedDict()
        self.bids: Dict[float, PriceLevel] = SortedDict()
        self.orders: Dict[UUID, Order] = {}
        self.tick_size = tick_size

    def clear(self):
        self.asks.clear()
        self.bids.clear()
        self.orders = {}

    def round(self, price: float):
        return round(price / self.tick_size) * self.tick_size

    def cancel_order(self, order_id):
        if order_id not in self.orders:
            raise KeyError(f"Order {order_id} not found")

        order = self.orders[order_id]
        side = self.asks if order.side == Side.SELL else self.bids

        level = side[order.price]
        level.pop(order)
        del self.orders[order_id]

    def _quantity(self, level):
        return sum(order.quantity for order in level)

    def quantity(self, price: float, side: Side):
        if side not in (Side.BUY, Side.SELL):
            raise ValueError("side must be Side.BUY or Side.SELL")

        side = self.asks if side == Side.SELL else self.bids
        price = self.round(price)
        level = side.get(price)
        return self._quantity(level) if level is not None else 0

    @property
    def shape(self):
        return len(self.bids), len(self.asks)

    def update_order(self, order):
        self.orders[order.uuid] = order

    @staticmethod
    def validate_order(order: Order):
        if order.price <= 0:
            raise ValueError("Price must be positive")
        if order.quantity <= 0:
            raise ValueError("Quantity must be positive")
        if order.side not in (Side.BUY, Side.SELL):
            raise ValueError("side must be Side.BUY or Side.SELL")

    def send_limit(self, order: Order):
        self.validate_order(order)

        order.price = self.round(order.price)  # think about a better way to do without editing input order
        side = self.asks if order.side == Side.SELL else self.bids

        level = side.get(order.price)
        if level is None:
            side[order.price] = PriceLevel(order.price)
            level = side[order.price]

        self.orders[order.uuid] = order
        level.add_order(order)

    def send_market(self, order: Order):
        self.validate_order(order)

        order.price = self.round(order.price)
        matching = self.bids if order.side == Side.SELL else self.asks

        transactions = []

        for price, level in matching.items():
            while not level.empty() and order.quantity > 0:
                best_order = level.top()
                transactions.append(Transaction(order.client, best_order.client, order.price, min(order.quantity, best_order.quantity)))
                diff = min(order.quantity, best_order.quantity)
                order.quantity -= diff
                best_order.quantity -= diff
                if best_order.quantity <= 0:
                    level.pop(best_order)

            if order.quantity <= 0:
                break

        return transactions

    def depth(self, n_levels, side: Side):
        if side not in (Side.BUY, Side.SELL):
            raise ValueError("side must be Side.BUY or Side.SELL")
        side = self.asks if side == Side.SELL else self.bids
        i = 0
        for price, level in side.items():
            i += 1
            if i > n_levels:
                return
            yield price, self._quantity(level)
