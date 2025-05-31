from typing import Type

from dxlib import Strategy, History, HistoryView


class PairsTrading(Strategy):
    def __init__(self, spread_signal: callable):
        self.spread_signal = spread_signal  # Function operating on a timestamp quote slice

    def execute(self,
                observation: History,
                history: History,
                history_view: Type[HistoryView],
                *args, **kwargs) -> History:
        result: History = history_view.apply(history, self.spread_signal)
        return result.loc(index=observation.data.index)

    def output_schema(self, observation: History):
        return {
            "spread": float,
            "zscore": float,
            "position": dict  # e.g., {"EUR/USD": +1, "USD/BRL": -1}
        }
