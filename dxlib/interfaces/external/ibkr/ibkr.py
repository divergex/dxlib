import threading
import time

from ibapi.client import EClient

from dxlib.interfaces import TradingInterface
from .ibkr_account import IbkrAccount
from .ibkr_market import IbkrMarket
from .wrapper import IbkrWrapper


class Ibkr(TradingInterface, EClient):
    def __init__(self, host: str, port: int, client_id: int, *args, **kwargs):
        self.conn = None
        self.thread = None
        self.wrapper = IbkrWrapper()
        EClient.__init__(self, wrapper=self.wrapper)

        self.host = host
        self.port = port
        self.client_id = client_id

        self.market_interface = IbkrMarket(self)
        self.account_interface = IbkrAccount(self)

    def start(self, timeout=10):
        self.connect(self.host, self.port, self.client_id)
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

        # sleep until connected
        while not self.isConnected() and timeout > 0:
            timeout -= 1
            time.sleep(1)

        if not self.isConnected():
            self.stop()
            raise Exception(f"Timeout connecting to {Ibkr.__name__}")

        return self.thread

    def stop(self):
        self.disconnect()
        self.thread.join()
        self.conn = None
        self.thread = None

    def assert_connected(self):
        assert self.isConnected(), f"Not connected to Interface {IbkrMarket.__name__}. See {self.start.__name__}"

    class _ConnectionManager:
        def __init__(self, ibkr_instance):
            self.ibkr = ibkr_instance

        def __enter__(self) -> "Ibkr":
            self.ibkr.start()
            self.ibkr.assert_connected()
            return self.ibkr

        def __exit__(self, exc_type, exc_value, traceback):
            self.ibkr.stop()

    @property
    def connection(self):
        return self._ConnectionManager(self)