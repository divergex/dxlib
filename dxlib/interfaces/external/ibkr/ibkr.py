import time
import threading
import numpy as np

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order


class TradeApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.prices = []
        self.next_order_id = None
        self.current_price_req_id = 1001
        self.open_orders = {}
        self.position = 0

    def start(self):
        self.connect("127.0.0.1", 4002, clientId=1)
        api_thread = threading.Thread(target=self.run)
        api_thread.start()
        time.sleep(1)
        self.reqIds(1)
        self.reqAccountUpdates(True, "DU8605718")
        self.reqPositions()

    def stop(self):
        self.disconnect()

    def nextValidId(self, orderId: int):
        self.next_order_id = orderId
        print(f"Next valid order ID: {self.next_order_id}")

    def request_current_price(self, symbol: str):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        self.reqMarketDataType(3)
        self.reqMktData(self.current_price_req_id, contract, "", False, False, [])
        self.current_price_req_id += 1

    def tickPrice(self, reqId: int, tickType: int, price: float, attrib):
        if tickType == 4:
            print(f"Tick Price. ReqId: {reqId}, Last Price: {price}")
            self.prices.append(price)
            if len(self.prices) >= 14:
                self.calculate_rsi()

    def calculate_rsi(self):
        prices = np.array(self.prices[-14:])
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)

        avg_gain = np.mean(gains)
        avg_loss = np.mean(losses)

        if avg_loss == 0:
            rsi = 100
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

        print(f"RSI: {rsi}")


        if rsi < 30:
            self.place_order("BUY")
        elif rsi > 70:
            self.place_order("SELL")

    def place_order(self, action: str):
        if self.next_order_id is None:
            print("Order ID is not available. Cannot place order.")
            return

        contract = Contract()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        order = self.create_order(action, 10)

        self.placeOrder(self.next_order_id, contract, order)
        print(f"Placed {action} order for 10 shares of AAPL with order ID: {self.next_order_id}.")

        self.next_order_id += 1

    def get_book(self, symbol: str):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        # cant use smart
        contract.exchange = "ISLAND"
        contract.currency = "USD"
        self.reqMktDepth(self.current_price_req_id, contract, 5, False, [])
        self.current_price_req_id += 1

    def updateMktDepth(self, reqId: int, position: int, operation: int, side: int, price: float, size: int):
        print(f"Market Depth. ReqId: {reqId}, Position: {position}, Operation: {operation}, Side: {side}, "
              f"Price: {price}, Size: {size}")

    def tickByTickBidAsk(self, reqId: int, time: int, bidPrice: float, askPrice: float, bidSize: int, askSize: int, tickAttribBidAsk):
        print(f"Bid Price: {bidPrice}, Ask Price: {askPrice}")

    def create_order(self, action: str, quantity: int):
        order = Order()
        order.action = action
        order.orderType = "MKT"
        order.totalQuantity = quantity
        return order

    def cancel_all_orders(self):
        for order_id in list(self.open_orders.keys()):
            self.cancelOrder(order_id)
            print(f"Canceled order ID: {order_id}")
        self.open_orders.clear()

    def openOrder(self, orderId: int, contract: Contract, order: Order, orderState: str):
        self.open_orders[orderId] = order
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

def format_currency(value):
    if value >= 1_000_000_000:  # 1 billion
        return f"${value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:  # 1 million
        return f"${value / 1_000_000:.2f}M"
    else:
        return f"${value:.2f}"


def main():
    app = TradeApp()

    app.start()

    app.request_current_price("AAPL")
    app.get_book("AAPL")

    try:

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        app.cancel_all_orders()
        print(f"Position: {format_currency(app.position)}")
        print("Exiting...")
        app.stop()


if __name__ == "__main__":
    main()
