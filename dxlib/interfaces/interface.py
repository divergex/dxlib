from abc import ABC

from .market_interface import MarketInterface


class Interface(ABC):
    market_interface: 'MarketInterface'
