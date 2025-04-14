from abc import ABC


class Interface(ABC):
    pass


class TradingInterface(Interface):
    market_interface = None
    account_interface = None
    order_interface = None

    def __init__(self, *args, **kwargs):
        pass

