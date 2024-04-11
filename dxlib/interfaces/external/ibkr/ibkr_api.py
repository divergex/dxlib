import threading
import time

from ibapi.common import BarData, TickerId, TickAttrib
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.ticktype import TickType
from ibapi.wrapper import EWrapper
from ibapi.client import EClient


class Wrapper(EWrapper):
    def __init__(self):
        super().__init__()
        self.data = []

    def historicalData(self, reqId, bar):
        self.data.append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume])

    # realtime quotes
    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float,
                  attrib: TickAttrib):
        self.data.append([tickType, price])

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


class Client(EClient):
    def __init__(self, wrapper: EWrapper = None):
        super().__init__(wrapper=wrapper or Wrapper())
        self.nextValidOrderId = 0

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def historical(self, ticker: str):
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        # set 15 min delay
        self.reqMarketDataType(4)
        self.reqHistoricalData(1, contract, "", "1 D", "1 min", "TRADES", 0, 1, False, [])

    def quote(self, ticker: str):
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        self.reqMarketDataType(4)
        self.reqMktData(1, contract, "", False, False, [])


class InteractiveBrokersAPI:
    def __init__(self, host: str = "127.0.0.1", port: int = 4002, client_id: int = 0):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.wrapper = Wrapper()
        self.client = Client(wrapper=self.wrapper)

    # decorator for connect and disconnect
    @staticmethod
    def interactive(func):
        def wrapper(self, *args, **kwargs):
            self.client.connect(self.host, self.port, self.client_id)
            func(self, *args, **kwargs)
            thread = threading.Thread(target=self.client.run)
            thread.start()

            while not self.wrapper.data:
                time.sleep(1)

            self.client.disconnect()
            thread.join()

        return wrapper

    @interactive
    def historical(self, ticker: str):
        self.client.historical(ticker)
        response = self.wrapper.data
        self.wrapper.data = []
        return response

    @interactive
    def quote(self, ticker: str):
        self.client.quote(ticker)
        response = self.wrapper.data
        self.wrapper.data = []
        return response

    def send_order(self, ticker, action, quantity, order_type):
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

        self.client.placeOrder(self.client.nextOrderId(), contract, order)
        response = self.wrapper.data
        self.wrapper.data = []
        return response

    def cancel_order(self, order_id):
        self.client.cancelOrder(order_id)
        response = self.wrapper.data
        self.wrapper.data = []
        return response


if __name__ == "__main__":
    api = InteractiveBrokersAPI()
    historical = api.historical("AAPL")
    print(historical)

    # quotes = api.quote("AAPL")
    # print(quotes)

    order = Contract()