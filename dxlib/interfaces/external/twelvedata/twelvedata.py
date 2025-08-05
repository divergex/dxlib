import datetime
from typing import Dict, Any, List

import httpx
import pandas as pd

from dxlib.core import Instrument, InstrumentStore
from dxlib.history import History, HistorySchema
from dxlib.interfaces import MarketInterface


class TwelveData(MarketInterface):
    def __init__(self, api_key=None):
        self._crumb = None
        self.client = None
        self.key = api_key

    @property
    def base_url(self) -> str:
        return "https://api.twelvedata.com"

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

    def start(self):
        self.client = httpx.Client(headers=self.headers, follow_redirects=True, timeout=10)

    def stop(self):
        if self.client is not None:
            self.client.close()
        self.client = None

    @property
    def history_schema(self) -> HistorySchema:
        return HistorySchema(
            index={'datetime': datetime.datetime, 'instrument': Instrument},
            columns={
                'close': float,
                'open': float,
                'high': float,
                'low': float,
                'volume': float
            }
        )

    def _format_history(self,
                        instrument: Instrument,
                        response: Dict[str, Any]
                        ) -> History:
        data = response['values']

        if not data:
            df = pd.DataFrame([], columns=self.history_schema.column_names)
            df.index = pd.MultiIndex.from_tuples([], names=self.history_schema.index_names)
            return History(self.history_schema, df)

        df: pd.DataFrame = pd.DataFrame(data)
        df['datetime'] = pd.to_datetime(df['datetime'])
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            df[col] = df[col].astype(float)

        df['symbol'] = instrument
        df = df.rename(columns={"symbol": "instrument"})

        df.set_index(['datetime', 'instrument'], inplace=True)
        return History(self.history_schema, df)

    def _historical(self, symbol: str, start: int, end: int, interval: str, version="v8") -> Dict[str, Any]:
        assert self.client is not None, "Start the Api instance first."
        url = f"{self.base_url}/time_series"
        params = {
            "symbol": symbol,
            "interval": interval,
            "apikey": self.key,
            "outputsize": 30,
        }
        r = self.client.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()

    def historical(self,
                   symbols: List[str] | str | Instrument | List[Instrument],
                   start: datetime.datetime,
                   end: datetime.datetime,
                   interval: str = '1d',
                   store: InstrumentStore = None,
                   ) -> History:
        assert self.client is not None, "Start the Api instance first."
        store = store or InstrumentStore()
        if isinstance(symbols, list):
            instruments = [store.setdefault(symbol, Instrument(symbol)) for symbol in symbols]
        else:
            instruments = [store.setdefault(symbols, Instrument(symbols))]
        history = History(history_schema=self.history_schema)

        for instrument in instruments:
            response = self._historical(
                instrument.symbol,
                int(start.timestamp()),
                int(end.timestamp()),
                interval
            )
            history.extend(self._format_history(instrument, response))

        return history