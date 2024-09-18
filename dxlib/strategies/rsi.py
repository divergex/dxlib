from enum import Enum

import pandas as pd

from .. import History, HistorySchema
from ..core import Strategy


def rsi(history, window=14):
    """
    # RSI = 100 - 100 / (1 + RS)
    # RS = Average Gain / Average Loss
    """
    # First, calculate the difference between the closing price of each day.
    delta = history["close"].diff()

    # Then, calculate the gains and losses.
    gains = delta.where(delta > 0, 0)
    losses = -delta.where(delta < 0, 0)

    # Calculate the average gain and the average loss.
    avg_gain = gains.rolling(window=window).mean()
    avg_loss = losses.rolling(window=window).mean()

    # Calculate the relative strength.
    rs = avg_gain / avg_loss

    # Calculate the relative strength index.
    rsi = 100 - 100 / (1 + rs)

    return rsi


class Signal(Enum):
    BUY = 1
    SELL = -1
    HOLD = 0


class RsiStrategy(Strategy):
    def __init__(self, output_schema: HistorySchema, window: int, upper: float, lower: float):
        super().__init__(output_schema)
        self.window = window
        self.upper = upper
        self.lower = lower

    def execute(self,
                history: History,
                observation: History,
                *args, **kwargs) -> History:
        """
        Execute trading signals based on the RSI indicator.
        """
        rsi_values = rsi(observation, window=self.window)
        signals = []
        for rsi_value in rsi_values:
            if rsi_value > self.upper:
                signals.append(Signal.SELL)
            elif rsi_value < self.lower:
                signals.append(Signal.BUY)
            else:
                signals.append(Signal.HOLD)

        return History(
            schema=self.output_schema,
            data={
                "index": observation.data.index,
                "index_names": observation.data.index.names,
                "columns": ["signal"],
                "column_names": [""],
                "data": signals,
            }
        )
