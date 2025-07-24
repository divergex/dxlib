import datetime

import httpx
import pandas as pd

from typing import Dict, Any, List
from dxlib.interfaces import MarketInterface
from dxlib.history import History, HistorySchema
from dxlib.core import Instrument, InstrumentStore


class YFinance(MarketInterface):
    def __init__(self, cookie = None):
        self.cookie = cookie
        self.cookies = {
            "A1": self.cookie,
            "A3": self.cookie,
            "A1S": self.cookie,
        }
        self._crumb = None
        self.client = None

    def start(self):
        self.client = httpx.Client(headers=self.headers, cookies=self.cookies)

    def stop(self):
        self.client.close()
        self.client = None

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
        return "https://query2.finance.yahoo.com"

    def crumb(self):
        if self._crumb:
            return self._crumb
        r = self.client.get("https://query2.finance.yahoo.com/v1/test/getcrumb")
        r.raise_for_status()
        self._crumb = r.text.strip()
        return self._crumb

    def _quote(self, symbols, crumb=None, version="v7"):
        crumb = crumb or self.crumb()
        symbols = ",".join(symbols)
        url = f"{self.base_url}/{version}/finance/quote"
        params = {
            "symbols": symbols,
            "crumb": crumb,
            "lang": "en-US",
            "region": "US",
            "formatted": "false",
        }
        r = self.client.get(url, params=params)
        r.raise_for_status()
        return r.json()

    def _format_quote(self, response_json):
        results = response_json.get("quoteResponse", {}).get("result", [])
        records = []
        for item in results:
            timestamp = item.get("regularMarketTime")
            ts = pd.to_datetime(timestamp, unit="s") if timestamp else pd.NaT
            symbol = item.get("symbol")
            bid = item.get("bid")
            ask = item.get("ask")
            market_price = item.get("regularMarketPrice")
            records.append({
                "timestamp": ts,
                "symbol": symbol,
                "bid": bid,
                "ask": ask,
                "marketPrice": market_price,
            })

        df = pd.DataFrame(records)
        df.set_index(["timestamp", "symbol"], inplace=True)
        return df

    def quote(self, symbols):
        assert self.cookie is not None, "This method requires loading cookies with `YFinance(cookie)`."
        assert self.client, "Start the Api instance first."
        return self._format_quote(self._quote(symbols))

    def _historical(self, symbol: str, start: int, end: int, interval: str, version="v8") -> Dict[str, Any]:
        url = f"{self.base_url}/{version}/finance/chart/{symbol}"
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
            index={'date': datetime.datetime, 'instruments': Instrument},
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
        result = response['chart']['result'][0]
        timestamps = result.get('timestamp', [])
        quote = result['indicators']['quote'][0]

        if not timestamps:
            df = pd.DataFrame([], columns=list(self.history_schema.columns.keys()))
            df.index = pd.MultiIndex.from_tuples([], names=list(self.history_schema.index.keys()))
            return History(self.history_schema, df)

        df = pd.DataFrame({
            'date': pd.to_datetime(timestamps, unit='s'),
            'instruments': instrument,
            'close': quote['close'],
            'open': quote['open'],
            'high': quote['high'],
            'low': quote['low'],
            'volume': quote['volume']
        })
        df['volume'] = df['volume'].astype(float)
        df.set_index(['date', 'instruments'], inplace=True)
        return History(self.history_schema, df)

    def historical(self,
                   symbols: List[str] | str | Instrument | List[Instrument],
                   start: datetime.datetime,
                   end: datetime.datetime,
                   interval: str = '1d',
                   store: InstrumentStore = None,
                   ) -> History:
        assert self.client, "Start the Api instance first."
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

    def _symbols(self, query: str, crumb=None, version="v1", lang="en-US") -> Dict[str, Any]:
        # crumb = crumb or self.crumb()
        url = f"{self.base_url}/{version}/finance/search"
        # quotesQueryId=tss_match_phrase_query&multiQuoteQueryId=multi_quote_single_token_query
        # enablePrivateCompany=true
        # enableLists=false
        params = {
            "q": query,
            "crumb": crumb,
            "quotesCount": 10,
            "quotesQueryId": "tss_match_prase_query",
            "multiQuotesQueryId": "multi_quote_single_token_query",
            "enablePrivateCompany": "true",
            "enableLists": "false",
            "lang": lang,
            "region": "US"
        }
        r = self.client.get(url, params=params)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def format_symbols(response: Dict[str, Any]):
        quotes = response["quotes"]
        symbols = []
        for quote in quotes:
            symbols.append(quote["symbol"])
        return symbols

    def symbols(self, query):
        return self.format_symbols(self._symbols(query))
