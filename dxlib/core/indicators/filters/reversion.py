from typing import Callable, Optional

import numpy as np
import pandas as pd

from dxlib import HistorySchema
from dxlib.history import History
from dxlib.core import Signal

def zscore(short_window: int = 5, long_window: int = 20) -> Callable[[pd.Series], pd.Series]:
    def _zscore(prices: pd.Series) -> pd.Series:
        short_ma = prices.rolling(short_window).mean()
        long_ma = prices.rolling(long_window).mean()
        long_std = prices.rolling(long_window).std()

        z = (short_ma - long_ma) / long_std
        return z.replace([np.inf, -np.inf], np.nan).fillna(0)
    return _zscore


class Reversion:
    def __init__(self, upper=1.5, lower=-1.5, score: Optional[Callable] = None):
        self.upper = upper
        self.lower = lower
        self.score = score if score is not None else zscore(5, 20)

    def get_signals(self, history: History) -> History:
        scores = history.apply({"instrument": self.score})

        signals = pd.DataFrame(index=scores.index, columns=scores.columns)

        for column in scores.columns:
            signals.loc[scores[column] > self.upper, column] = Signal.SELL
            signals.loc[scores[column] < self.lower, column] = Signal.BUY

        schema = HistorySchema(
            index=history.history_schema.index,
            columns={col: Signal for col in history.history_schema.column_names},
        )

        return History(
            history_schema=schema,
            data=signals.dropna()
        )
