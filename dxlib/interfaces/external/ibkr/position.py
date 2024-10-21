from ibapi.client import *
from ibapi.wrapper import *
from ibapi.contract import Contract
import time
import threading


class TradeApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.stop = threading.Event()

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        # print("UpdateAccountValue. Key:", key, "Value:", val, "Currency:", currency, "AccountName:", accountName)
        # print only non-zero values
        try:
            if key == "NetLiquidationByCurrency" and float(val) != 0:
                print("UpdateAccountValue. Key:", key, "Value:", val, "Currency:", currency, "AccountName:", accountName)
                self.stop.set()
            # if float(val) != 0:
            #     print("UpdateAccountValue. Key:", key, "Value:", val, "Currency:", currency, "AccountName:", accountName)
        except ValueError:
            pass

    def updatePortfolio(self, contract: Contract, position: Decimal, marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float, realizedPNL: float, accountName: str):
        # print("UpdatePortfolio.", "Symbol:", contract.symbol, "SecType:", contract.secType, "Exchange:",
        #       contract.exchange, "Position:", decimalMaxString(position), "MarketPrice:", floatMaxString(marketPrice),
        #       "MarketValue:", floatMaxString(marketValue), "AverageCost:", floatMaxString(averageCost),
        #       "UnrealizedPNL:", floatMaxString(unrealizedPNL), "RealizedPNL:", floatMaxString(realizedPNL),
        #       "AccountName:", accountName)
        # print only non-zero positions
        if position != 0:
            print("UpdatePortfolio.", "Symbol:", contract.symbol, "Position:", position, "MarketPrice:", marketPrice,
                  "MarketValue:", marketValue, "AverageCost:", averageCost, "UnrealizedPNL:", unrealizedPNL,
                  "RealizedPNL:", realizedPNL, "AccountName:", accountName)


    def updateAccountTime(self, timeStamp: str):
        print("UpdateAccountTime. Time:", timeStamp)

    def accountDownloadEnd(self, accountName: str):
        print("AccountDownloadEnd. Account:", accountName)


app = TradeApp()
app.connect("127.0.0.1", 4002, clientId=1)
time.sleep(1)
app.reqAccountUpdates(True, 'DU8605718')

api_thread = threading.Thread(target=app.run)

api_thread.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    app.disconnect()
    api_thread.join()
    print("Finished")