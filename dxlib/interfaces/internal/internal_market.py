import json
from datetime import datetime

import httpx
from dxlib.history import History
from dxlib.interfaces import Server
from httpx import ConnectError

from ..market_interface import MarketInterface


class MarketInterfaceInternal(MarketInterface):
    def __init__(self):
        self.server: Server | None = None

    def register(self, server: Server):
        self.server = server

    def historical(self, symbols: list[str], start: datetime, end: datetime, interval: str) -> History:
        try:
            request = httpx.post(f"{self.server.url}/historical",
                                 json={"symbols": symbols,
                                       "start": start.isoformat(),
                                       "end": end.isoformat(),
                                       "interval": interval})
            request.raise_for_status()
            return History.from_dict(json.loads(request.json()))
        except ConnectError:
            raise ConnectionError("Could not connect to the server")
