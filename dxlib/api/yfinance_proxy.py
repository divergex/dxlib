import importlib
from typing import Type


# Define the interface for type hints
class SocketAPI:
    def __init__(self):
        pass

    async def get_data(self):
        pass

    def listen(self):
        pass


class YFinanceProxy:
    def __init__(self):
        self._module = None

    def _load_module(self):
        if self._module is None:
            self._module = importlib.import_module('dxlib.interfaces.external.yfinance')

    def __getattr__(self, name: str):
        self._load_module()
        return getattr(self._module, name)


yfinance = YFinanceProxy()
YFinance = Type[yfinance]
