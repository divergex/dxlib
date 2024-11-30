import threading
from datetime import datetime
from enum import Enum
from typing import Any, Callable

import pandas as pd

from ibapi.client import EClient
from ibapi.contract import Contract

from dxlib.core.history import History, HistorySchema
from .wrapper import IbkrWrapper
from ...market_interface import MarketInterface


class OrderType(Enum):
    MARKET = "MKT"
    LIMIT = "LMT"
    STOP = "STP"
    STOP_LIMIT = "STP LMT"


class IbkrMarket(MarketInterface):
    def __init__(self, client: EClient):
        self.client = client
        self.wrapper: IbkrWrapper = client.wrapper
        self.thread = None

        self.requests = {}

    def execute(self):
        # run self.run in thread and return
        self.thread = threading.Thread(target=self.client.run)
        self.thread.start()
        return self.thread

    def _quote(self, symbol: str):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        req_id = self.wrapper.next_order_id or 1000
        self.client.reqMarketDataType(3)
        self.client.reqMktData(req_id, contract, "", False, False, [])
        return req_id

    def _historical(self, symbol: str, callback: Callable[[Any], None] = None):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        req_id = self.wrapper.next_order_id or 1000
        self.client.reqHistoricalData(req_id, contract, "", "1 Y", "1 day", "TRADES", 0, 1, False, [])
        return req_id

    def historical(self, symbols: list[str], start: datetime, end: datetime, interval: str) -> History:
        assert len(symbols) == 1, "Only one symbol is supported"
        assert self.client.isConnected()

        req_id = self._historical(symbols[0])
        self.requests.setdefault("historical", []).append(req_id)

        while not self.wrapper.get_end(req_id):
            pass

        schema = HistorySchema(
            index={"date": datetime},
            columns={
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": int,
                "bar_count": int
            }
        )

        data = self.wrapper.get_data(req_id)
        df = pd.DataFrame(data, columns=["date", "open", "high", "low", "close", "volume", "bar_count"])
        try:
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d %H:%M:%S %Z")
        except ValueError:
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
        df = df.set_index("date")
        history = History(schema, df)

        return history

    def cancel(self):
        for req_id in self.requests["historical"]:
            self.client.cancelHistoricalData(req_id)
        self.requests = {}