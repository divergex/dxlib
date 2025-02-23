.. _rsi:

RSI Strategy Documentation
==========================

Strategy Overview
-----------------

The RSI strategy is a momentum-based trading strategy that uses the Relative Strength Index (RSI) indicator
to determine overbought and oversold conditions in the market.
It generates buy and sell signals based on the RSI value, aiming to capture short-term price movements.

Usage
-----

To use the RSI strategy, initialize it with the following parameters:

.. code-block:: python

    from dxlib.strategy import RSI
    from dxlib.history import History, HistorySchema
    import pandas as pd

    schema = HistorySchema(index={"date": "datetime64[ns]"}, columns={"close": "float64"})
    data = pd.DataFrame({"date": ["2024-01-01", "2024-01-02"], "close": [100.5, 101.0]})
    history = History(history_schema=schema, data=data)

    rsi = RsiStrategy(window=2, overbought=70, oversold=30)

    rsi.execute(history)
    print(rsi.data)

    >>>                           signal
    >>> security date
    >>> AAPL     2021-01-01  Signal.HOLD
    >>>          2021-01-02  Signal.SELL
    >>>          2021-01-03  Signal.HOLD
    >>>          2021-01-04  Signal.HOLD
    >>>          2021-01-05  Signal.HOLD
    >>>          2021-01-06  Signal.HOLD
    >>>          2021-01-07  Signal.HOLD

