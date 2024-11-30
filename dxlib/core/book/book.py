from typing import Dict


class OrderBook:
    """
    Order book for given symbol and exchange.
    """
    def __init__(self):
        self.tick_size = 2
        self.asks = {}
        self.bids = {}

    def set(self, asks: Dict[float, int], bids: Dict[float, int]):
        """
        Set the order book data.
        """
        self.asks = asks
        self.bids = bids
