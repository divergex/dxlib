from typing import List

from dxlib.market import Order, OrderTransaction


class BacktestOrderInterface:
    def __init__(self):
        pass

    @staticmethod
    def send(orders: List[Order]):
        transactions = []
        for order in orders:
            transactions.append(OrderTransaction(order))
        return transactions
