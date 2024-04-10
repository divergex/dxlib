import threading

from ibapi.common import BarData
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper
from ibapi.client import EClient


class Wrapper(EWrapper):
    def __init__(self):
        super().__init__()
        self.data = []

    def historicalData(self, reqId, bar):
        self.data.append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume])


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

        self.reqHistoricalData(1, contract, "", "1 D", "1 min", "TRADES", 0, 1, False, [])

        # self.run() and self.wrapper.run() are blocking calls
        # so call and break when data is received
        thread = threading.Thread(target=self.run)
        thread.start()

        while not self.wrapper.data:
            pass
        thread.join()

        return self.wrapper.data


class Interactive:
    def __init__(self):
        self.client = Client()

    def historical(self, ticker: str):
        return self.client.historical(ticker)


# connect 127.0.0.1 4001 0
ib = Interactive()
ib.client.connect("127.0.0.1", 4001, 0)
data = ib.historical("AAPL")
ib.client.disconnect()
print(data)
