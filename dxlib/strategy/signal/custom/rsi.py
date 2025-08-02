import numpy as np
import pandas as pd

from dxlib.core import Signal
from dxlib.history import HistorySchema
from ..signal_generator import SignalGenerator


def fast_rsi(values: np.ndarray, window: int):
    rsi = np.full(values.shape, np.nan, dtype=np.float64)
    for col in range(values.shape[1]):
        gains = np.zeros(window)
        losses = np.zeros(window)

        for i in range(1, window + 1):
            delta = values[i, col] - values[i - 1, col]
            gains[i - 1] = max(delta, 0)
            losses[i - 1] = max(-delta, 0)

        avg_gain = np.mean(gains)
        avg_loss = np.mean(losses)

        for i in range(window + 1, values.shape[0]):
            delta = values[i, col] - values[i - 1, col]
            gain = max(delta, 0)
            loss = max(-delta, 0)

            avg_gain = (avg_gain * (window - 1) + gain) / window
            avg_loss = (avg_loss * (window - 1) + loss) / window

            rs = avg_gain / avg_loss if avg_loss != 0 else np.nan
            rsi[i, col] = 1 - (1 / (1 + rs)) if rs == rs else np.nan

    return rsi


class Rsi(SignalGenerator):
    def __init__(self, window=14, lower=0.3, upper=0.7, reverting=True, period=None):
        self.window = window
        self.period = period or 1
        self.lower = lower
        self.upper = upper
        self.up = Signal.SELL if reverting else Signal.BUY
        self.down = Signal.BUY if reverting else Signal.SELL

        assert (0 <= lower <= 1) and (0 <= upper <= 1) and (lower < upper)

    def generate(self, data: pd.DataFrame, history_schema: HistorySchema):
        score = self.score(data)
        conditions = [score < self.lower, score > self.upper]
        choices = [self.down, self.up]
        output_schema = self.output_schema(history_schema)
        return pd.DataFrame(np.select(conditions, choices, default=Signal.HOLD), index=score.index, columns=output_schema.column_names)

    def _score(self, data: pd.DataFrame):
        group = data.tail(self.period + self.window)

        delta = group.diff().dropna()

        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        avg_gain = gain.rolling(window=self.window, min_periods=1).mean()
        avg_loss = loss.rolling(window=self.window, min_periods=1).mean()

        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 1 - (1 / (1 + rs))

        return rsi.tail(self.period).fillna(self.upper)

    def score(self, data: pd.DataFrame):
        group = data.tail(self.period + self.window).to_numpy(dtype=np.float64)
        rsi = fast_rsi(group, self.window)
        rsi = pd.DataFrame(rsi, index=data.tail(self.period + self.window).index, columns=data.columns)
        return rsi.tail(self.period).fillna(self.upper)

    @classmethod
    def output_schema(cls, history_schema: HistorySchema):
        schema = history_schema.copy()
        schema.columns = {column: Signal for column in schema.column_names}
        return schema
