import pandas as pd

from dxlib.core import Signal
from dxlib.history import History


class Momentum:
    def __init__(self, long_window=252, skip_window=21, threshold=0.0):
        self.long_window = long_window
        self.skip_window = skip_window
        self.threshold = threshold

    def momentum(self, prices: pd.Series) -> pd.Series:
        shifted = prices.shift(self.skip_window)
        long = prices.shift(self.long_window + self.skip_window)
        return (shifted / long) - 1

    def get_signals(self, history: History) -> History:
        df = history.data
        momentum_df = df.apply(self.momentum)

        signals = pd.DataFrame(index=df.index, columns=df.columns)

        for column in df.columns:
            signals[column] = Signal.HOLD
            signals.loc[momentum_df[column] > self.threshold, column] = Signal.BUY
            signals.loc[momentum_df[column] < -self.threshold, column] = Signal.SELL

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
