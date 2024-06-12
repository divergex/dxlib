import threading
import time
from typing import AsyncGenerator

from ibapi.common import TickerId, TickAttrib
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.ticktype import TickType, TickTypeEnum
from ibapi.wrapper import EWrapper
from ibapi.client import EClient


class TickTypeWrapper:
    DELAYED_LAST = "delayed_last"
    DELAYED_HIGH = "delayed_high"
    DELAYED_LOW = "delayed_low"
    DELAYED_CLOSE = "delayed_close"
    DELAYED_OPEN = "delayed_open"
    DELAYED_BID = "delayed_bid"
    DELAYED_ASK = "delayed_ask"

    @classmethod
    def to_str(cls, tick_type: TickType):
        val = TickTypeEnum.to_str(tick_type).upper()
        return cls.__dict__.get(val, val.lower())


class Wrapper(EWrapper):
    def __init__(self):
        super().__init__()
        self.data = []

    # noinspection PyPep8Naming
    def historicalData(self, reqId, bar):
        self.data.append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume])

    # noinspection PyPep8Naming
    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float,
                  attrib: TickAttrib):
        self.data.append([TickTypeWrapper.to_str(tickType), price])

    # noinspection PyPep8Naming
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice,
                    clientId, whyHeld, mktCapPrice):
        self.data.append({
            "orderId": orderId,
            "status": status,
            "filled": filled,
            "remaining": remaining,
            "avgFillPrice": avgFillPrice,
            "lastFillPrice": lastFillPrice,
            "whyHeld": whyHeld
        })

    def nextValidId(self, orderId: int):
        self.data.append(orderId)

    def clear(self):
        self.data = []


class Aggregator:
    """
    Callbacks to the API interactive methods

    For example, the quote method requires that the appended data be transformed into a dictionary given tickType,
    but since responses from the IB API are iterative, we the aggregation step is necessary.
    """

    def __init__(self, wrapper: Wrapper):
        self.wrapper = wrapper

    def quote(self):
        response = dict(self.wrapper.data)
        self.wrapper.clear()

        return response

    def next_id(self):
        response = self.wrapper.data.pop()
        self.wrapper.clear()

        return response

    def order(self):
        response = dict(self.wrapper.data)
        self.wrapper.clear()

        return response


class Client(EClient):
    def __init__(self, wrapper: EWrapper = None):
        super().__init__(wrapper=wrapper or Wrapper())

    def historical(self, ticker: str):
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        # set 15 min delay
        self.reqMarketDataType(4)
        self.reqHistoricalData(1, contract, "", "1 D", "1 min",
                               "TRADES", 0, 1, False, [])

    def quote(self, ticker: str):
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        self.reqMarketDataType(4)
        self.reqMktData(1, contract, "", False, False, [])

    def next_id(self):
        self.reqIds(1)

    def send_order(self, order_id, contract, order_data):
        self.placeOrder(order_id, contract, order_data)


class InteractiveBrokersAPI:
    def __init__(self, host: str = "127.0.0.1", port: int = 4002, client_id: int = 0):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.wrapper = Wrapper()
        self.client = Client(wrapper=self.wrapper)
        self.aggregator = Aggregator(self.wrapper)

    def interactive(self, func, *args, **kwargs):
        self.client.connect(self.host, self.port, self.client_id)

        try:
            response = func(*args, **kwargs)
        except Exception as e:
            self.client.disconnect()
            raise e

        thread = threading.Thread(target=self.client.run)
        thread.start()

        while not self.wrapper.data:
            time.sleep(1)

        self.client.disconnect()
        thread.join()

        return self.wrapper.data, response

    def historical(self, ticker: str):
        return self.interactive(self.client.historical, ticker)

    def quote(self, ticker: str):
        self.interactive(self.client.quote, ticker)
        return self.aggregator.quote()

    def _send_order(self, ticker, action, quantity, order_type):
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = order_type
        order.eTradeOnly = ''
        order.firmQuoteOnly = ''

        return contract, order

    def next_id(self):
        self.interactive(self.client.next_id)
        return self.aggregator.next_id()

    def send_order(self, order_id, ticker, action, quantity, order_type):
        contract, order_data = self._send_order(ticker, action, quantity, order_type)
        order = self.interactive(self.client.send_order, order_id, contract, order_data)
        return order

    def cancel_order(self, order_id):
        return self.interactive(self.client.cancelOrder, order_id)

    def stream(self, ticker) -> AsyncGenerator:
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        self.client.reqMarketDataType(4)
        self.client.reqMktData(1, contract, "", False, False, [])
        while True:
            if self.wrapper.data:
                yield self.wrapper.data.pop()
            time.sleep(1)


if __name__ == "__main__":
    api = InteractiveBrokersAPI()
    historical = api.historical("AAPL")
    print(historical)

    # quotes = api.quote("AAPL")
    # print(quotes)

    order = Contract()
