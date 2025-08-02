import pandas as pd

from dxlib.market import Signal
from dxlib.history import HistorySchema
from dxlib.strategy.signal import SignalGenerator, SignalStrategy


class WickReversal(SignalGenerator):
    def __init__(self,
                 range_multiplier=0.4,
                 low_range=1,
                 up_range=1,
                 close_multiplier=0.05,
                 close_high = 1,
                 close_low = 1):
        self.low_range = low_range * range_multiplier
        self.up_range = up_range * range_multiplier
        self.close_high = 1 - close_high * close_multiplier
        self.close_low = 1 + close_low * close_multiplier

    def generate(self, data: pd.DataFrame, input_schema: HistorySchema) -> pd.DataFrame:
        body = data['close'] - data['open']
        range_ = data['high'] - data['low']

        upper_wick = data['high'] - data[['close', 'open']].max(axis=1)
        lower_wick = data[['close', 'open']].min(axis=1) - data['low']

        bullish = (
                (body > 0) &
                (lower_wick > range_ * self.low_range) &
                (data['close'] >= data['high'] * self.close_high)
        )

        bearish = (
                (body < 0) &
                (upper_wick > range_ * self.up_range) &
                (data['close'] <= data['low'] * self.close_low)
        )

        signal = pd.Series(Signal.HOLD, index=data.index)
        signal[bullish] = Signal.BUY
        signal[bearish] = Signal.SELL

        return pd.DataFrame({
            'signal': signal
        }, index=data.index)

    @classmethod
    def output_schema(cls, history_schema: HistorySchema):
        schema = history_schema.copy()
        schema.columns = {"signal": Signal}
        return schema
