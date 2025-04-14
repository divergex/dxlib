import datetime
import httpx
import pandas as pd

from typing import Dict, Any, Union, List
from dxlib.interfaces import market_interface
from dxlib.history import History, HistorySchema
from dxlib.core import Security


class YFinance(market_interface.MarketInterface):
    def __init__(self):
        self.client = httpx.Client(headers=self.headers)

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }

    @property
    def base_url(self) -> str:
        return "https://query1.finance.yahoo.com/v8/finance/chart"

    def _request(self, symbol: str, start: int, end: int, interval: str) -> Dict[str, Any]:
        url = f"{self.base_url}/{symbol}"
        params = {
            "interval": interval,
            "period1": str(start),
            "period2": str(end),
            "events": "capitalGain|div|split",
            "formatted": "true",
            "includeAdjustedClose": "true",
            "lang": "en-US",
            "region": "US"
        }
        r = self.client.get(url, params=params)
        r.raise_for_status()
        return r.json()

    @property
    def history_schema(self) -> HistorySchema:
        return HistorySchema(
            index={'date': datetime.datetime, 'security': Security},
            columns={
                'close': float,
                'open': float,
                'high': float,
                'low': float,
                'volume': float
            }
        )

    def _format_history(self,
                        symbol: str,
                        response: Dict[str, Any]
                        ) -> History:
        result = response['chart']['result'][0]
        timestamps = result.get('timestamp', [])
        quote = result['indicators']['quote'][0]

        if not timestamps:
            df = pd.DataFrame([], columns=list(self.history_schema.columns.keys()))
            df.index = pd.MultiIndex.from_tuples([], names=list(self.history_schema.index.keys()))
            return History(self.history_schema, df)

        df = pd.DataFrame({
            'date': pd.to_datetime(timestamps, unit='s'),
            'security': symbol,
            'close': quote['close'],
            'open': quote['open'],
            'high': quote['high'],
            'low': quote['low'],
            'volume': quote['volume']
        })
        df.set_index(['date', 'security'], inplace=True)
        return History(self.history_schema, df)

    def historical(self,
                   symbols: List[str] | str,
                   start: datetime.datetime,
                   end: datetime.datetime,
                   interval: str = '1d'
                   ) -> History:
        symbols = symbols if isinstance(symbols, list) else [symbols]
        history = History(history_schema=self.history_schema)

        for symbol in symbols:
            response = self._request(
                symbol,
                int(start.timestamp()),
                int(end.timestamp()),
                interval
            )
            history.extend(self._format_history(symbol, response))

        return history
