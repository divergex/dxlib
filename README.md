# dxlib - a Quantitative Analysis Framework
[![Publish Package](https://github.com/delphos-quant/dxlib/actions/workflows/publish-package.yml/badge.svg?branch=main&event=release)](https://github.com/delphos-quant/dxlib/actions/workflows/publish-package.yml)
![PyPI - Downloads](https://img.shields.io/pypi/dm/dxlib)
![GitHub Tag](https://img.shields.io/github/v/tag/delphos-quant/dxlib)
![GitHub License](https://img.shields.io/github/license/delphos-quant/dxlib)

> `dxlib` offers a rich set of tools tailored for traders, researchers, and quantitative analysts, covering everything from basic statistical methods to comprehensive trading simulations.
> It emphasizes a unified interface for fetching and analyzing data, ensuring consistent and predictable outputs irrespective of data sources.

## Installation

Use the package manager [pip](https://pip.pypa.io/) to install dxlib.

```bash
pip install dxlib
```

## Modules Overview

### Interfaces
Encapsulates a series of financial data api, allowing static data requests, data streaming and websocket creation.
Currently available APIs endpoints are:
* Alpaca Markets
* Alpha Vantage
* Yahoo! Finance

Unified data-fetching methods, ensuring consistent data structures.
To use the individual API endpoints, simply load the respective API module:

```python
from dxlib.interfaces.api import YFinanceAPI

api = YFinanceAPI()

top_symbols = api.get_symbols(n=100)
historical_bars = api.historical_prices(top_symbols["symbol"].values)

print(top_symbols)
print(historical_bars)
```
```
   symbol    trade_count        volume
0    MCOM          86713     146651863
1    EBET          33428      83560053
...
98   FNGD           9886       5580585
99   OPEN          13278       5528415
```
```
Fields                  Close                   ...       VWAP      Volume            
Ticker                  AAL      AAPL   ABCM    ...       XPEV         XLF        XPEV
Time                                            ...                                   
2022-08-29 04:00:00     13.51  160.43  14.99    ... 
...
2023-08-25 04:00:00     NaN      NaN     NaN    ...  17.149326  37520857.0  19358445.0
```

### Simulation Module

Harness `dxlib` to simulate trading strategies:

```python
import dxlib as dx
from dxlib import StrategyManager, Portfolio
from dxlib.interfaces.api import YFinanceAPI

data = YFinanceAPI().get_historical_bars(["AAPL", "MSFT", "GOOGL", "AMZN"], start="2022-01-01", end="2022-12-31")
portfolio = Portfolio().add_cash(1e4)
my_strategy = StrategyManager(dx.strategies.RsiStrategy())
my_strategy.register(portfolio)
historical_signals = my_strategy.run(data["Close"])
```

**Highlight: Built-in Server**

The simulation manager features an embedded server that serves requests related to the simulation process. This allows for real-time monitoring and potential interaction with running simulations. Check out the `Server` class in the `simulation` module for more details.

Start the server with `simulation_manager.start_server()`. Check the logs for server activity and exception handling.

```python

import dxlib as dx
from dxlib.interfaces.servers import SimulationManager

data = dx.api.YFinanceAPI().get_historical_bars(["AAPL", "MSFT", "GOOGL", "AMZN"], start="2022-01-01", end="2022-12-31")
my_strategy = dx.strategies.RsiStrategy()

logger = dx.info_logger("simulation_manager")
my_manager = SimulationManager(my_strategy, use_server=True, port=5000, logger=logger)

my_manager.start()

try:
  while my_manager.server.is_alive():
    with my_manager.server.exceptions as exceptions:
      if exceptions:
        logger.exception(exceptions)
except KeyboardInterrupt:
  pass
finally:
  my_manager.stop_server()
```

```bash
$ python servers.py
2023-08-15 04:11:37,417 - INFO - Server starting on port 5000 (http_server.py:308)
2023-08-15 04:11:37,422 - INFO - Server started on port 5000. Press Ctrl+C to stop (http_server.py:292)
127.0.0.1 - - [15/Aug/2023 04:12:01] "GET / HTTP/1.1" 200 -
127.0.0.1 - - [15/Aug/2023 04:12:09] "GET / HTTP/1.1" 200 -
127.0.0.1 - - [15/Aug/2023 04:13:35] "GET / HTTP/1.1" 200 -
127.0.0.1 - - [15/Aug/2023 04:14:18] "POST /add_cash HTTP/1.1" 200 -
127.0.0.1 - - [15/Aug/2023 04:15:38] "GET /portfolio HTTP/1.1" 200 -
```

### Trading Module

```python
from dxlib.strategies import RandomForestStrategy as rfs
from dxlib.data import DataLoader

features, labels = DataLoader(data["Close"])
x_train, y_train, x_test, y_test = train_test_split(features, labels, 0.5)
clf = rfs().fit(x_train, y_train)
y_pred = rfs.predict(x_test)
print(f"Returns: {sum(y_pred - y_test}")
```

### `History` Class

**Description:**  
The `History` class is a robust stock market data handler, offering a suite of utilities and technical indicators for market analysis.

- **Data Management**: Seamlessly manage your stock price data. Easily add new symbols or rows of data.

- **Timeseries Indicators**: Common indicators and timeseries methods:
    - Moving Averages
    - Exponential Moving Averages
    - Logarithmic Changes
    - Volatility Measurements
   
- **Technical Indicators**: Enjoy a rich suite of built-in technical indicators, such as (Convert your History to JSON format with a single call to the `to_json` method):
    - RSI
    - Sharpe Ratio
    - Bollinger Bands
    - Beta
    - Drawdown Computations

- **Extendability**: Extend the `History` class with more technical indicators or data analysis tools as per your requirements.

Here's a quick start guide to using the `History` class:

```python
from dxlib import History
import pandas as pd

# Sample data
data = pd.DataFrame({
    'AAPL': [150, 151, 152],
    'MSFT': [300, 301, 302]
})

history = History(data)

# Adding a new stock ticker
history.add_security('TSLA', [650, 651, 652])

# Calculating moving averages
moving_average = history.indicators.sma(window=2)
print(moving_average)
```

## Contributions

We welcome improvements and extensions:

1. Fork the repository.
2. Make your enhancements.
3. Submit a pull request.

Ensure thorough testing of all changes. Your contributions will make `dxlib` even better!
