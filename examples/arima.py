import datetime

from dxlib import History, Cache
from statsmodels.tsa.arima.model import ARIMA

from dxlib.interfaces.external.yfinance.yfinance import YFinance


def main():
    market_api = YFinance()
    cache = Cache(".dx")
    storage = "market_data"

    symbols = ["AAPL", "MSFT", "PETR4.SA", "BBAS3.SA"]
    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2024, 12, 31)

    history = cache.cached(storage, History, market_api.historical, symbols, start, end)
    grouped = history.data.groupby(level='instruments')

    forecast = {}
    error = {}
    dt = 5
    for security, ts in grouped:
        ts = ts['close'].reset_index(level='instruments', drop=True)
        ts = ts.asfreq('B')
        model = ARIMA(ts, order=(1, 1, 1))
        fitted = model.fit()
        exp = fitted.get_forecast(steps=dt)
        forecast[security] = exp.predicted_mean
        error[security] = exp.conf_int()

    for security, ts in forecast.items():
        print(f"Security: {security}")
        print(ts)
        print("Confidence Interval:")
        print(error[security])
        print()

if __name__ == "__main__":
    main()
