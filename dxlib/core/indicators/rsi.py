import pandas as pd

from dxlib.history import History
from dxlib.core.signal import Signal


class RSI:
    def __init__(self, window=14, upper=70, lower=30):
        self.window = window
        self.upper = upper
        self.lower = lower

    def rsi(self, df: pd.DataFrame):
        """
        # RSI = 100 - 100 / (1 + RS)
        # RS = Average Gain / Average Loss
        """
        delta = df.diff()

        gains = delta.where(delta > 0, 0)
        losses = -delta.where(delta < 0, 0)

        avg_gain = gains.rolling(window=self.window).mean()
        avg_loss = losses.rolling(window=self.window).mean()

        rs = avg_gain / avg_loss

        return 100 - 100 / (1 + rs)

    def get_signals(self, history: History) -> History:
        df = history.data
        rsi_values = self.rsi(df)

        signals = pd.DataFrame(index=history.data.index, columns=history.data.columns)

        for column in rsi_values:
            signals[column] = Signal.HOLD
            signals.loc[rsi_values[column] > self.upper, column] = Signal.SELL
            signals.loc[rsi_values[column] < self.lower, column] = Signal.BUY

        return History(
            history_schema=history.history_schema,
            data={
                "index": history.data.index,
                "index_names": history.data.index.names,
                "columns": signals.columns,
                "column_names": [""],
                "data": signals,
            }
        )
