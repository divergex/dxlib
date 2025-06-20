from typing import Type

from dxlib import History, benchmark
from ..strategy import Strategy
from ..history_view import HistoryView
from .signal_generator import SignalGenerator


class SignalStrategy(Strategy):
    def __init__(self, signal: SignalGenerator, history_view = None):
        self.signal = signal
        self.history_view = history_view

    def execute(self,
                observation: History,
                history: History,
                history_view: Type[HistoryView] = None,
                *args, **kwargs) -> History:
        history_view = history_view or self.history_view
        assert history_view is not None
        result: History = history_view.apply(history, self.signal.generate)
        return result.loc(index=observation.data.index)

    def output_schema(self, observation: History):
        return self.signal.output_schema(observation)
