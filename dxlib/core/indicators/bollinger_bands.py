import pandas as pd

from dxlib.history import History, HistorySchema
from ..signal import Signal


class BolllingerBands:
    def __init__(self, window=20, std=2):
        self.window = window
        self.std = std
        self.output_schema = HistorySchema(
            index={"date": pd.Timestamp, "instruments": str},
            columns={"close": float}
        )

    def bollinger_bands(self, df: pd.DataFrame):
        """
        Bollinger Bands
        """
        rolling_mean = df.rolling(window=self.window).mean()
        rolling_std = df.rolling(window=self.window).std()

        upper_band = rolling_mean + (rolling_std * self.std)
        lower_band = rolling_mean - (rolling_std * self.std)

        return upper_band, lower_band

    def get_signals(self, history: History) -> History:
        df = history.data
        bands = self.bollinger_bands(df)
        signals = []

        for i in range(len(df)):
            if df.iloc[i] > bands[0].iloc[i]:
                signals.append(Signal.SELL)
            elif df.iloc[i] < bands[1].iloc[i]:
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
