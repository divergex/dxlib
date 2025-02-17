InvestingCom Class
==================

This class implements the :class:`MarketInterface` and provides methods to interact with the Investing.com API for retrieving market data such as historical data, quotes, and symbols.

Attributes
----------
- ``scrapper`` (cloudscraper): A scraper object for making HTTP requests.
- ``url`` (str): The base URL for API requests.
- ``headers`` (dict): HTTP headers for requests to the API.
- ``history_schema`` (:class:`HistorySchema`): Schema defining the structure of historical data.

Methods
-------

__init__(self)
--------------
Initializes the ``InvestingCom`` object by setting up the ``scrapper`` for HTTP requests.

url
----
.. method:: url(self) -> str

    Returns the URL for API requests. The URL is dynamically generated using a UUID.

headers
-------
.. method:: headers(self) -> dict

    Returns the headers required for the API requests, including a User-Agent and Content-Type.

get
---
.. method:: get(self, url: str, params: Dict[str, Any], headers: Dict[str, str]) -> requests.Response

    Makes a GET request using the ``scrapper`` to the provided URL with the given parameters and headers.

_request
---------
.. method:: _request(self, endpoint: Literal["history.py", "search", "quotes", "symbols"], params: Dict[str, Any]) -> Union[Dict[str, Any], List[Dict[str, Any]]]

    Sends a request to the Investing.com API for a given endpoint (e.g., "history.py", "search", "quotes", "symbols").
    Returns the response as a JSON object or raises a :class:`ConnectionError` if the request fails.

history_schema
--------------
.. method:: history_schema(self)

    Returns the schema used to structure historical market data. It defines index columns (``date``, ``security``) and data columns (``close``, ``open``, ``high``, ``low``, ``volume``).

_format_history
---------------
.. method:: _format_history(self, params: Dict[str, Any], response: Dict[str, Any]) -> History

    Formats the response from the API into a :class:`History` object using the ``history_schema``.

_historical
-----------
.. method:: _historical(self, params: Dict[str, Any]) -> History

    Retrieves historical market data for a symbol or a list of symbols. The method supports retrieving data for multiple symbols and combines the results into a :class:`History` object.

historical
-----------
.. method:: historical(self, symbols: List[str] | str, start: datetime.datetime, end: datetime.datetime, interval: Literal[1, 5, 15, 30, 60, 'D', 'W', 'M'] = 'D')

    Retrieves historical market data for the specified symbols and date range. The data resolution can be specified (e.g., 'D' for daily, 'M' for monthly).

search
------
.. method:: search(self, params: Dict[str, Any]) -> List[Dict[str, Any]]

    Searches for symbols based on the provided query parameters (e.g., ``query``, ``limit``).

quotes
------
.. method:: quotes(self, params: Dict[str, Any]) -> Dict[str, Any]

    Retrieves the latest quotes for a specified symbol.

symbols
-------
.. method:: symbols(self, params: Dict[str, Any]) -> List[Dict[str, Any]]

    Retrieves symbols based on the provided parameters (e.g., ``symbols``).

listen
------
.. method:: listen(self, symbols: List[str], interval: float = 60) -> AsyncGenerator

    Asynchronously listens to market data by periodically fetching the latest quotes for the specified symbols. The data is returned as an async generator.

Example Usage
-------------
The following example demonstrates how to use the ``InvestingCom`` class to fetch historical data and search for a symbol.


.. doctest::

    >>> from dxlib.interfaces.external import investing_com as ext
    >>> import datetime

    >>> api = ext.InvestingCom()
    >>> history = api.historical(
    >>>     symbols="AAPL",
    >>>     start=datetime.datetime(2021, 1, 1),
    >>>     end=datetime.datetime(2021, 12, 31)
    >>> )

    >>> history.head()
                                  close        high  ...        open       volume
    date       security                          ...
    2021-01-04 AAPL      129.410004  133.610001  ...  133.520004  143301888.0
    2021-01-05 AAPL      131.009995  131.740005  ...  128.889999   97664896.0
    2021-01-06 AAPL      126.599998  131.050003  ...  127.720001  155087968.0
    2021-01-07 AAPL      130.919998  131.630005  ...  128.360001  109578160.0
    2021-01-08 AAPL      132.050003  132.630005  ...  132.429993  105158248.0

    [5 rows x 5 columns]q
