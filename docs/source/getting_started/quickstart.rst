Quickstart
==========

Usage
-----
Defining the financial instruments and formats
You can use the dxlib.Schema and dxlib.SecurityManager classes to define the financial instruments and formats.

.. exec_code::

    import dxlib as dx

    tickers = ['AAPL', 'GOOG', 'MSFT']

    security_manager = dx.SecurityManager.from_list(tickers)
    print(security_manager)

    schema = dx.Schema(
        levels=[dx.SchemaLevel.SECURITY],
        fields=['price'],
        security_manager=security_manager
    )
    print(schema)

Creating a history of prices
You can use the ``dxlib.History`` class to store the history of a stock.

.. exec_code::

    import dxlib as dx
    import datetime

    data = {
        (datetime.datetime(2015, 1, 1), 'AAPL'): {'open': 100, 'high': 105, 'low': 95, 'close': 100, 'volume': 1000000},
        (datetime.datetime(2015, 1, 2), 'AAPL'): {'open': 100, 'high': 105, 'low': 95, 'close': 100, 'volume': 1000000},
    }

    schema = dx.Schema(
        levels=[dx.SchemaLevel.DATE, dx.SchemaLevel.SECURITY],
        fields=['open', 'high', 'low', 'close', 'volume'],
        security_manager=dx.SecurityManager.from_list(['AAPL'])
    )

    history = dx.History(data, schema)
    print(history)


Executing a strategy
--------------------

.. exec_code::

    import dxlib as dx

    strategy = dx.strategies.RsiStrategy()

    tickers = ['AAPL', 'GOOG', 'MSFT']
    security_manager = dx.SecurityManager.from_list(tickers)

    schema = dx.Schema(
        levels=[dx.SchemaLevel.DATE, dx.SchemaLevel.SECURITY],
        fields=['open', 'high', 'low', 'close', 'volume'],
        security_manager=security_manager
    )


