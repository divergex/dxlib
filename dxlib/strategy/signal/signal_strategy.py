from typing import Type

from dxlib import History
from .order_generator import OrderGenerator
from ..strategy import Strategy
from ..history_view import HistoryView
from .signal_generator import SignalGenerator


class SignalStrategy(Strategy):
    def __init__(self, signal: SignalGenerator, order: OrderGenerator):
        self.signal = signal
        self.order = order

    def execute(self,
                observation: History,
                history: History,
                history_view: Type[HistoryView],
                *args, **kwargs) -> History:
        signals: History = history_view.apply(history, self.signal.generate)
        orders = self.order.generate(signals.data.reset_index("security"))
        return orders.loc(index=observation.data.index)

    def output_schema(self, observation: History):
        return self.signal.output_schema(observation)
