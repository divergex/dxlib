from decimal import Decimal

from ibapi.common import TickerId
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper

from ..utils import CallbackBase, StoreBase


class IbkrBaseWrapper(EWrapper, CallbackBase, StoreBase):
    def __init__(self):
        EWrapper.__init__(self)
        CallbackBase.__init__(self)
        StoreBase.__init__(self)


class IbkrMarketWrapper(IbkrBaseWrapper):
    def __init__(self):
        super().__init__()
        self.next_order_id = None

    def nextValidId(self, order_id: int):
        self.next_order_id = order_id

    @CallbackBase.callback("reqId")
    @StoreBase.store("reqId")
    def historicalData(self, reqId: int, bar):
        return {
            "date": bar.date,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "bar_count": bar.barCount
        }

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        self.set_end(reqId)

    @CallbackBase.callback("reqId")
    @StoreBase.store("reqId")
    def tickByTickData(self, reqId: int, tickType: int, timeStamp: int, price: float, size: int,
                       exchange: str, specialConditions: str):
        return tickType, timeStamp, price, size, exchange, specialConditions

    def cancelTickByTickData(self, reqId: int):
        self.set_end(reqId)

    @CallbackBase.callback("reqId")
    @StoreBase.store("reqId")
    def updateMktDepth(self, reqId: int, position: int, operation: int, side: int, price: float, size: int):
        pass

    def cancelMktDepth(self, reqId: TickerId, isSmartDepth: bool):
        self.set_end(reqId)

    @CallbackBase.callback("reqId")
    @StoreBase.store("reqId")
    def tickPrice(self, reqId: int, tickType: int, price: float, attrib):
        return {"tickType": tickType, "price": price}

    @CallbackBase.callback("reqId")
    @StoreBase.store("reqId")
    def tickSize(self, reqId: int, tickType: int, size: int):
        return {"tickType": tickType, "size": size}

    def cancelMktData(self, reqId: int):
        self.set_end(reqId)

    def tickSnapshotEnd(self, reqId:int):
        self.set_end(reqId)

class IbkrAccountWrapper(IbkrBaseWrapper):
    def __init__(self):
        super().__init__()

    @StoreBase.store("position")
    def position(self, account: str, contract: Contract, position: Decimal, avgCost: float):
        if position == 0:
            return
        return {
            "symbol": contract.symbol,
            "position": position,
            "avgCost": avgCost
        }

    def positionEnd(self):
        print("Position End")
        self.set_end("position")

    @StoreBase.store("account")
    def managedAccounts(self, accountsList:str):
        self.set_end("account")
        return accountsList.split(",")

class IbkrWrapper(IbkrMarketWrapper, IbkrAccountWrapper):
    pass

