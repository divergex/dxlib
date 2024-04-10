from __future__ import annotations

import datetime
import time

from ibapi.common import BarData
from .ContractSamples import ContractSamples

try:
    from ibapi.client import EClient
    from ibapi.contract import Contract
    from ibapi.wrapper import EWrapper
except ImportError:
    raise ImportError("""
    IBKR API is not installed. Please install it using `pip install ibapi`
    
    Please refer to the official documentation for more information:
        - https://www.interactivebrokers.com/en/trading/ib-api.php
        
    Note: dxlib does not provide the API key for IBKR. You need to have your own API key to use this API.
    dxlib is also not responsible for any charges or fees incurred while using the IBKR API.
    """)


class Wrapper(EWrapper):
    def __init__(self):
        super().__init__()
        self.data = []

    def historicalData(self, reqId, bar):
        self.data.append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume])


class Client(EClient):
    def __init__(self, wrapper: EWrapper = None):
        super().__init__(wrapper=wrapper or Wrapper())


class InteractiveBrokersApi:
    def __init__(self):
        self.wrapper = Wrapper()
        self.client = Client(wrapper=self.wrapper)

        self.con = Contract()

    def connect(self, host: str = "127.0.0.1", port: int = 4002, client_id: int = 0):
        self.client.connect(host, port, client_id)

    def disconnect(self):
        self.client.disconnect()

    @property
    def connected(self):
        return self.client.isConnected()

    def historical(self, ticker):
        self.connect()

        self.con.symbol = ticker
        self.con.secType = "STK"
        self.con.exchange = "SMART"
        self.con.currency = "USD"

        self.client.reqHistoricalData(1, self.con, "", "1 D", "1 min", "TRADES", 0, 1, False, [])
        self.client.run()

        self.disconnect()

        return self.get_data()

    def get_data(self):
        return self.wrapper.data