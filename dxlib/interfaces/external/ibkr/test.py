from threading import Thread

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order


class TestApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = []

    def orderStatus(self, orderId: int, status: str, filled: float, remaining: float,
                    avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float,
                    clientId: int, whyHeld: str, mktCapPrice: float):
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
              "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
              "LastFillPrice:", lastFillPrice, "WhyHeld:", whyHeld)
        self.data = {
            "orderId": orderId,
            "status": status,
            "filled": filled,
            "remaining": remaining,
            "avgFillPrice": avgFillPrice,
            "lastFillPrice": lastFillPrice,
            "whyHeld": whyHeld
        }

    def openOrder(self, orderId: int, contract: Contract, order: Order, orderState):
        print("OpenOrder. ID:", orderId, contract.symbol, contract.secType,
              "@", contract.exchange, ":", order.action, order.orderType, order.totalQuantity, orderState.status)
        self.data = {
            "orderId": orderId,
            "contract": contract,
            "order": order,
            "orderState": orderState
        }

    def execDetails(self, reqId, contract, execution):
        print("ExecDetails. ", reqId, contract.symbol, contract.secType, "ExecPrice:", execution.price,
              "ExecTime:", execution.time)


def create_contract(symbol, secType, exchange, currency):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = secType
    contract.exchange = exchange
    contract.currency = currency
    return contract


def create_order(action, quantity, orderType):
    order = Order()
    order.action = action
    order.totalQuantity = quantity
    order.orderType = orderType
    # Remove eTradeOnly
    order.eTradeOnly = ''
    # Remove FirmQuoteOnly
    order.firmQuoteOnly = ''
    return order


def main():
    app = TestApp()

    app.connect("127.0.0.1", 4002, clientId=0)
    # Example: Create contract and order
    contract = create_contract("AAPL", "STK", "SMART", "USD")
    order = create_order("BUY", 100, "MKT")

    order.orderId = 1

    app.placeOrder(1, contract, order)

    thread = Thread(target=app.run)
    thread.start()

    while not app.data:
        pass

    print(app.data)
    app.data = []

    app.disconnect()
    thread.join()
    print("\n\n")
    # delete order, get current open orders
    app.connect("127.0.01", 4002, clientId=0)
    app.cancelOrder(1)

    thread = Thread(target=app.run)
    thread.start()

    while not app.data:
        pass

    print(app.data)
    app.data = []

    app.disconnect()
    thread.join()




if __name__ == "__main__":
    main()
