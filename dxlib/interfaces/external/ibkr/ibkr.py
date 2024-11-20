import time
import threading
from datetime import datetime
from enum import Enum

import pandas as pd

from dxlib.core.history import History, HistorySchema
from dxlib.interfaces import MarketInterface

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order


class OrderType(Enum):
    MARKET = "MKT"
    LIMIT = "LMT"
    STOP = "STP"
    STOP_LIMIT = "STP LMT"


class IbkrWrapper(EClient, EWrapper):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.next_order_id = None
        self.sent_orders = {}
        self.data = []
        self.position = 0
        self.orders = []
        self.done = False
        self.current_price_req_id = 1001

    def nextValidId(self, orderId: int):
        self.next_order_id = orderId
        print(f"Next valid order ID: {self.next_order_id}")

    def place_order(self, symbol: str, action: str, quantity: int, order_type: OrderType, price: float = 0):
        if self.next_order_id is None:
            print("Order ID is not available. Cannot place order.")
            return

        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        order = Order()
        order.action = action
        order.orderType = order_type.value
        order.totalQuantity = quantity
        order.lmtPrice = price

        self.placeOrder(self.next_order_id, contract, order)
        print(f"Placed {action} order for {quantity} shares of {symbol} with order ID: {self.next_order_id}.")

        self.next_order_id += 1

    def cancel_all_orders(self):
        for order_id in list(self.sent_orders.keys()):
            self.cancelOrder(order_id)
            print(f"Canceled order ID: {order_id}")
        self.sent_orders.clear()

    def openOrder(self, orderId: int, contract: Contract, order: Order, orderState: str):
        self.sent_orders[orderId] = order
        print(f"Open Order. ID: {orderId}, Symbol: {contract.symbol}, Action: {order.action}, "
              f"Status: {orderState}")

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        try:
            if key == "NetLiquidationByCurrency" and float(val) != 0:
                self.position = float(val)
            else:
                return
        except ValueError:
            pass

    def request_current_price(self, symbol: str):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        self.reqMarketDataType(3)
        self.reqMktData(self.current_price_req_id, contract, "", False, False, [])
        self.current_price_req_id += 1

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print("Historical Data End")
        self.done = True

    def get_historical_data(self, symbol: str):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        self.reqHistoricalData(1, contract, "", "1 Y", "1 day", "TRADES", 0, 1, False, [])

    # process reqHistoricalData
    def historicalData(self, reqId, bar):
        self.data.append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.barCount])

    def get_book(self, symbol: str):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        # cant use smart
        contract.exchange = "ISLAND"
        contract.currency = "USD"
        self.reqMktDepth(self.current_price_req_id, contract, 5, False, [])
        self.current_price_req_id += 1

    # order statuses
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice,
                    clientId, whyHeld, mktCapPrice):
        self.orders.append({
            "orderId": orderId,
            "status": status,
            "filled": filled,
            "remaining": remaining,
            "avgFillPrice": avgFillPrice,
            "permId": permId,
            "parentId": parentId,
            "lastFillPrice": lastFillPrice
        })
        print(f"Order ID: {orderId}, Status: {status}, Filled: {filled}, Remaining: {remaining}")

class IbkrMarket(IbkrWrapper, MarketInterface):
    def start(self):
        self.connect("127.0.0.1", 4002, clientId=1)
        api_thread = threading.Thread(target=self.run)
        api_thread.start()
        time.sleep(1)
        self.reqIds(1)
        self.reqPositions()

    def stop(self):
        self.disconnect()

    def historical(self, symbols: list[str], start: datetime, end: datetime, interval: str) -> History:
        # Test if connection is established
        if not self.isConnected():
            self.start()

        self.get_historical_data(symbols[0])
        while not self.data:
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
        df = pd.DataFrame(self.data, columns=["date", "open", "high", "low", "close", "volume", "bar_count"])
        # convert date index to correct format
        # 20241029 19:27:00 US/Eastern -> 2024-10-29 19:27:00
        try:
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d %H:%M:%S %Z")
        except ValueError:
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
        df = df.set_index("date")
        history = History(schema, df)
        self.data.clear()
        self.done = False
        self.stop()
        return history

    def portfolio(self, account:str, subscribe: bool = True):
        if not self.isConnected():
            self.start()

        self.reqAccountUpdates(subscribe, account)

        while not self.position:
            pass

        self.stop()
        return self.position


    def get_orders(self):
        # show currently open orders
        if not self.isConnected():
            self.start()

        self.reqOpenOrders()

        while not (self.orders or self.sent_orders):
            pass

        self.stop()
        return self.orders + list(self.sent_orders.values())

class Ibkr:
    def __init__(self):
        self.market_interface = IbkrMarket()
