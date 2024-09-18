from abc import ABC, abstractmethod
from enum import Enum


class AssetClass(Enum):
    STOCK = "stock"
    OPTION = "option"
    FUTURE = "future"
    CASH = "cash"
    INDEX = "index"
    ETF = "etf"
    CRYPTO = "crypto"


class Security:
    def __init__(self, symbol: str, name: str = None, asset_class: AssetClass = None):
        self.symbol = symbol
        self.name = name
        self.asset_class = asset_class

    @property
    @abstractmethod
    def value(self):
        raise NotImplementedError

    @value.setter
    def value(self, value):
        raise NotImplementedError

    @abstractmethod
    def to_dict(self):
        raise NotImplementedError

    @abstractmethod
    def from_dict(self, data):
        raise NotImplementedError

    def __json__(self):
        return self.to_dict()

    def __str__(self):
        return f"{self.__class__.__name__}({self.symbol}, {self.value})"


if __name__ == "__main__":
    class MySecurity(Security):
        def __init__(self, symbol: str, price):
            super().__init__(symbol)
            self.price = price

        @property
        def value(self):
            return self.price

        @value.setter
        def value(self, value):
            self.price = value

        def to_dict(self):
            return {"value": self.value}

        def from_dict(self, data):
            self.value = data["value"]

    s = MySecurity("AAPL", 100)
    print(s)

    import json
    print(s.to_dict())
    s_json = json.dumps(s, default=lambda x: x.__json__())
    print(s_json)
