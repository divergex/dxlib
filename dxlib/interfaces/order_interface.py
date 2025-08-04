from abc import ABC, abstractmethod
from typing import List, Dict

from dxlib.market import Order, OrderTransaction


class OrderInterface(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def send(self, orders: List[Order]):
        pass
    
    @abstractmethod
    def transactions(self) -> Dict[str, OrderTransaction]:
        pass
