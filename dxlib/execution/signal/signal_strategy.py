from typing import Type

from dxlib import History, benchmark
from ..strategy import Strategy
from ..history_view import HistoryView
from .signal_generator import SignalGenerator


class SignalStrategy(Strategy):
    def __init__(self, signal: SignalGenerator):
        self.signal = signal

    def execute(self,
                observation: History,
                history: History,
                history_view: Type[HistoryView],
                *args, **kwargs) -> History:
        result: History = history_view.apply(history, self.signal.generate)
        return result.loc(index=observation.data.index)

    def output_schema(self, observation: History):
        return self.signal.output_schema(observation)
