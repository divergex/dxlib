.. _ibkr_market:

IBKR Market
===========

Overview
--------

The IBKR Market interface provides access to historical and real-time market data from Interactive Brokers.

.. code-block:: python

    >>> self.api = ibkr.Ibkr("127.0.0.1", 4002, 0)
    >>> self.api.start()

    >>> symbols = ["AAPL"]
    >>> history = self.api.market_interface.historical(symbols, "2021-01-01", "2021-02-01", "D")
    >>> print(history)

    Index: {'date': <class 'datetime.datetime'>},
    Columns: {'open': <class 'float'>, 'high': <class 'float'>, 'low': <class 'float'>, 'close': <class 'float'>, 'volume': <class 'int'>, 'bar_count': <class 'int'>},
                  open    high     low   close   volume  bar_count
    date
    2024-02-23  183.94  185.35  182.23  182.28   333090     144711
    2024-02-26  182.27  182.76  180.65  181.03   335256     131140
    2024-02-27  180.60  183.93  179.56  183.06   448855     191479
    2024-02-28  183.30  183.47  180.13  181.22   405216     163556
    2024-02-29  180.89  182.57  179.53  180.63  1053960     266290
    ...            ...     ...     ...     ...      ...        ...
    2025-02-14  241.98  245.35  240.47  244.65   298642     127144
    2025-02-18  245.05  245.51  241.84  245.37   302596     117741
    2025-02-19  245.00  246.01  243.16  244.80   218317      92070
    2025-02-20  243.98  246.78  243.50  245.60   237629     103929
    2025-02-21  245.55  248.69  245.00  245.00   366301     145610

    [250 rows x 6 columns]


Class Autodoc
-------------

.. autoclass:: dxlib.interfaces.external.ibkr.ibkr_market.IbkrMarket
    :members:
    :inherited-members: