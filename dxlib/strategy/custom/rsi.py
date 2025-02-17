from dxlib import History, HistorySchema
from ..strategy import Strategy
from ..signal import Signal


def rsi(history, window=14):
    """
    # RSI = 100 - 100 / (1 + RS)
    # RS = Average Gain / Average Loss
    """
    delta = history["close"].diff()

    gains = delta.where(delta > 0, 0)
    losses = -delta.where(delta < 0, 0)

    avg_gain = gains.rolling(window=window).mean()
    avg_loss = losses.rolling(window=window).mean()

    rs = avg_gain / avg_loss

    return 100 - 100 / (1 + rs)


class RsiStrategy(Strategy):
    # TODO: Make a custom schema for this strategy. Should have a date and a security index.
    def __init__(self, output_schema: HistorySchema, window: int, upper: float, lower: float):
        super().__init__(output_schema)
        self.window = window
        self.upper = upper
        self.lower = lower

    def execute(self,
                observation: History=None,
                history: History=None,
                *args, **kwargs) -> History:
        """
        Execute trading signals based on the RSI indicator.
        """
        rsi_values = rsi(history, window=self.window)
        signals = []
        for rsi_value in rsi_values:
            if rsi_value > self.upper:
                signals.append(Signal.SELL)
            elif rsi_value < self.lower:
                signals.append(Signal.BUY)
            else:
                signals.append(Signal.HOLD)

        return History(
            history_schema=self.output_schema,
            data={
                "index": history.data.index,
                "index_names": history.data.index.names,
                "columns": ["signal"],
                "column_names": [""],
                "data": signals,
            }
        )
