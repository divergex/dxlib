import pandas as pd

from dxlib import HistorySchema
from dxlib.core import Signal
from dxlib.history import History


class Volatility:
    def __init__(self, window=21, quantile=0.2):
        self.window = window
        self.quantile = quantile

    def volatility(self, prices: pd.Series) -> pd.Series:
        return prices.pct_change().rolling(self.window).std()

    def get_signals(self, history: History) -> History:
        vol = history.apply({"instrument": self.volatility})

        signals = pd.DataFrame(index=vol.data.index, columns=vol.columns)
        threshold = vol.apply({"date": lambda x: x.quantile(self.quantile)}).data

        for date in vol.level_values("date"):
            row = vol.get({"date": [date]})
            new = row.data >= threshold.loc[date]
            signals.loc[new.any(axis=1)[lambda x: x].index] = Signal.BUY

        schema = HistorySchema(
            index=history.history_schema.index,
            columns={col: Signal for col in history.history_schema.column_names},
        )

        return History(
            history_schema=schema,
            data=signals.dropna()
        )
