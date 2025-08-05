import pandas as pd

from dxlib.core import Signal
from dxlib.history import History


class Volatility:
    def __init__(self, window=21, quantile=0.2):
        self.window = window
        self.quantile = quantile

    def volatility(self, prices: pd.Series) -> pd.Series:
        return prices.pct_change().rolling(self.window).std()

    def get_signals(self, history: History) -> History:
        df = history.data
        vol_df = df.apply(self.volatility)

        signals = pd.DataFrame(index=df.index, columns=df.columns)

        for date in df.index:
            vols = vol_df.loc[date]
            if vols.isna().all():
                continue
            threshold = vols.quantile(self.quantile)
            for asset in df.columns:
                signals.at[date, asset] = (
                    Signal.BUY if vols[asset] <= threshold else Signal.HOLD
                )

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
