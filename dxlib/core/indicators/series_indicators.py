from typing import Union

import numpy as np
import pandas as pd

from .indicators import Indicators


class SeriesIndicators(Indicators):
    @classmethod
    def sma(cls, series, window=20):
        ma = series.rolling(window=window).mean()
        ma.iloc[0] = series.iloc[0]
        return ma

    @classmethod
    def ema(cls, series, window=20):
        return series.ewm(span=window, adjust=False).mean()

    @classmethod
    def diff(cls, series, period=1):
        return series.diff(period)

    @classmethod
    def detrend(cls, series):
        return series - cls.sma(series)

    @classmethod
    def returns(cls, series):
        return series.pct_change()

    @classmethod
    def log_change(cls, series, window=1):
        rolling_change = series / series.shift(window)
        return np.log(rolling_change)

    @classmethod
    def relative_log_change(cls, series, window=1):
        relative_change = series / series.rolling(window).sum()
        return np.log(relative_change)

    @classmethod
    def autocorrelation(cls, series, lag=15):
        if isinstance(series, pd.DataFrame):
            df = series.apply(cls.autocorrelation, lag=lag)
            # Convert to a list of autocorrelation values
            return df.iloc[0].tolist()
        else:
            return series.autocorr(lag=lag)

    @classmethod
    def pacf(cls, series, lag_range=15) -> Union[pd.Series, pd.DataFrame]:
        if isinstance(series, pd.DataFrame):
            pacf_series = pd.DataFrame(index=range(lag_range), columns=series.columns)
            for column in series.columns:
                pacf_series[column] = cls.pacf(series[column], lag_range=lag_range)
            return pacf_series
        else:
            pacf_series = pd.Series(index=range(lag_range))
            for i in range(lag_range):
                pacf_series.iloc[i] = cls.autocorrelation(series, lag=i)
            return pacf_series
