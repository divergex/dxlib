import asyncio
import datetime
from typing import Literal, Dict, Any, Union, List, AsyncGenerator
from uuid import uuid4

try:
    import pandas as pd
    import requests
    import cloudscraper
except ImportError as e:
    raise ImportError(
        "Missing dependencies. Please install the required dependencies by running 'pip install dxlib[external.investing_com]'.")

from dxlib.interfaces import market_interface
from dxlib.history import History, HistorySchema
from dxlib.core import Instrument, InstrumentStore


class InvestingCom(market_interface.MarketInterface):
    def __init__(self):
        self.scrapper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'android',
                'desktop': False
            },
        )

    @property
    def url(self) -> str:
        return f"https://tvc6.investing.com/{uuid4().hex}/0/0/0/0/"

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like"
                " Gecko) Chrome/104.0.5112.102 Safari/537.36"
            ),
            "Referer": "https://tvc-invdn-com.investing.com/",
            "Content-Type": "application/json",
        }

    def get(self, url: str, params: Dict[str, Any], headers: Dict[str, str]) -> requests.Response:
        return self.scrapper.get(url, params=params, headers=headers)

    def _request(
            self,
            endpoint: Literal["history.py", "search", "quotes", "symbols"],
            params: Dict[str, Any]
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        url = self.url + endpoint
        headers = self.headers

        request = self.get(url, params=params, headers=headers)

        response = request.json()
        if len(response) > 0 and not (request.status_code == 200 and (
        response["s"] == "ok" if endpoint in ["history.py", "quotes"] else True)):
            raise ConnectionError(
                f"Request to Investing.com API failed with error message: {response['s']}."
                if "nextTime" not in response
                else f"Rejected time or quote for {params['symbol']} "
                     f"at {datetime.datetime.fromtimestamp(params['from'])}, "
                     f"try at {datetime.datetime.fromtimestamp(response['nextTime'])} (Did you invert the start and end dates?)"
            )

        return response

    @property
    def history_schema(self):
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
                        params: Dict[str, Any],
                        response: Dict[str, Any]
                        ) -> History:
        schema = self.history_schema

        if len(response) == 0:
            data = pd.DataFrame([], columns=list(schema.columns.keys()))
            # set index names
            data.index = pd.MultiIndex.from_tuples([], names=list(schema.index.keys()))
            return History(schema, data)

        data = pd.DataFrame({
            'date': pd.to_datetime(response['t'], unit='s'),
            'instruments': params['symbol'],
            'close': response['c'],
            'open': response['o'],
            'high': response['h'],
            'low': response['l'],
            'volume': response['v']
        })

        data.set_index(['date', 'instruments'], inplace=True)

        return History(schema, data)

    def _historical(self, params: Dict[str, Any]) -> History:
        """
        Get historical data for a symbol.

        Args:
            params (Dict[str, Any]): Parameters for the request.
                symbols (str | list): Symbol of the asset.
                resolution (str): Resolution of the data.
                from (str): Start date.
                to (str): End date.
        """
        if isinstance(params['symbols'], str):
            response = self._request("history.py", params)
            return self._format_history(params, response)
        else:
            history = History(history_schema=self.history_schema)

            param = params.copy()
            param = {k: v for k, v in param.items() if k != 'symbols'}

            for symbol in params.get('symbols', []):
                param['symbol'] = symbol
                response = self._request("history.py", param)
                history.extend(self._format_history(param, response))
            return history

    def historical(self,
                   symbols: List[str] | str,
                   start: datetime.datetime,
                   end: datetime.datetime,
                   interval: Literal[1, 5, 15, 30, 60, 'D', 'W', 'M'] = 'D',
                   store: InstrumentStore = None
                   ):
        return self._historical({
            'symbols': symbols if isinstance(symbols, list) else [symbols],
            'resolution': interval,
            'from': int(start.timestamp()),
            'to': int(end.timestamp())
        })

    def search(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Search for a symbol.

        Args:
            params (Dict[str, Any]): Parameters for the request.
                query (str): Query to search.
                limit (int): Limit of results.
        """
        return self._request("search", params)

    def quotes(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get quotes for a symbol.

        Args:
            params (Dict[str, Any]): Parameters for the request.
                symbol (str): Symbol of the asset.
        """
        return self._request("quotes", params)

    def symbols(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Get symbols.

        Args:
            params (Dict[str, Any]): Parameters for the request.
                symbols (str): Symbols of the assets.
        """
        return self._request("symbols", params)

    async def listen(self,
                     symbols: List[str],
                     interval: float = 60,
                     ) -> AsyncGenerator:
        """
        Listen to market data.
        """
        # Every <interval> seconds, get the latest quotes for the symbols and return as an async generator.
        while True:
            for symbol in symbols:
                yield self.quotes({"symbol": symbol})
            await asyncio.sleep(interval)
