from abc import ABC, abstractmethod
from typing import List

from dxlib.market import Order


class OrderInterface(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def send(self, orders: List[Order]):
        pass
