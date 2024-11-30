import threading

from ibapi.client import EClient
from ibapi.wrapper import EWrapper


class TradeApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.stop = threading.Event()
