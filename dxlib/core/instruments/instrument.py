from enum import Enum
from typing import Optional

from dxlib.types import TypeRegistry


class AssetClass(Enum):
    STOCK = "stock"
    OPTION = "option"
    FUTURE = "future"
    CASH = "cash"
    INDEX = "index"
    ETF = "etf"
    CRYPTO = "crypto"


class Instrument(TypeRegistry):
    symbol: str

    def __init__(self, symbol: str, name: Optional[str] = None, asset_class: Optional[AssetClass] = None, tick_size = 1):
        self.symbol = symbol
        self.name = name
        self.asset_class = asset_class
        self.tick_size = tick_size

    def __str__(self):
        return f"{self.__class__.__name__}({self.symbol})"

    def __repr__(self):
        return f"{self.__class__.__name__}({self.symbol})"

    def __eq__(self, other):
        return type(self) == type(other) and self.symbol == other.symbol

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return type(self) == type(other) and self.symbol < other.symbol

    def __hash__(self):
        return hash((self.symbol, self.name, self.asset_class))
