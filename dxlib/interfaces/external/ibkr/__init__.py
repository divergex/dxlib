from asyncio import Protocol

from dxlib.module_proxy import ModuleProxy
from dxlib.interfaces import TradingInterface
from .ibkr import Ibkr

ibkr = ModuleProxy("dxlib.interfaces.external.ibkr.ibkr")

class IbkrProtocol(TradingInterface, Protocol):
    def __init__(self, host: str, port: int, client_id: int, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def connection(self):
        return
