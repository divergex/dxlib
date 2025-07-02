import pandas as pd

from dxlib.core import Signal
from dxlib.history import HistorySchema
from dxlib.strategy.signal import SignalGenerator


class WickReversal(SignalGenerator):
    def generate(self, data: pd.DataFrame) -> pd.DataFrame:
        body = data['close'] - data['open']
        range_ = data['high'] - data['low']

        upper_wick = data['high'] - data[['close', 'open']].max(axis=1)
        lower_wick = data[['close', 'open']].min(axis=1) - data['low']

        bullish = (
                (body > 0) &
                (lower_wick > range_ * 0.4) &
                (data['close'] >= data['high'] * 0.95)
        )

        bearish = (
                (body < 0) &
                (upper_wick > range_ * 0.4) &
                (data['close'] <= data['low'] * 1.05)
        )

        signal = pd.Series(Signal.HOLD, index=data.index)
        signal[bullish] = Signal.BUY
        signal[bearish] = Signal.SELL

        return pd.DataFrame({
            'signal': signal
        }, index=data.index)


    def output_schema(self, history_schema: HistorySchema):
        return history_schema
