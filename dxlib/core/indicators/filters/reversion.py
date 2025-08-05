from typing import Callable, Optional

import numpy as np
import pandas as pd

from dxlib.history import History
from dxlib.core import Signal

def zscore(short_window, long_window):
    def _zscore(prices: pd.Series) -> pd.Series:
        short_ma = prices.rolling(short_window).mean()
        long_ma = prices.rolling(long_window).mean()
        long_std = prices.rolling(long_window).std()

        z = (short_ma - long_ma) / long_std
        return z.replace([np.inf, -np.inf], np.nan).fillna(0)
    return _zscore


class Reversion:
    def __init__(self, upper=1.5, lower=-1.5, score: Optional[Callable] = zscore):
        self.upper = upper
        self.lower = lower
        self.score = score if score is not None else zscore(5, 20)

    def get_signals(self, history: History) -> History:
        df = history.data
        z_df = df.apply(self.score)

        signals = pd.DataFrame(index=df.index, columns=df.columns)

        for column in df.columns:
            signals[column] = Signal.HOLD
            signals.loc[z_df[column] > self.upper, column] = Signal.SELL
            signals.loc[z_df[column] < self.lower, column] = Signal.BUY

        return History(
            history_schema=history.history_schema,
            data={
                "index": df.index,
                "index_names": df.index.names,
                "columns": signals.columns,
                "column_names": [""],
                "data": signals,
            }
        )
