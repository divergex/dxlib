from typing import Type

from dxlib.history import History, HistorySchema, HistoryView
from dxlib.market import Order
from ..order_generator import OrderGenerator
from ..strategy import Strategy
from . import SignalGenerator


class SignalStrategy(Strategy):
    def __init__(self, signal_generator: SignalGenerator, order_generator: OrderGenerator):
        self.signal_generator = signal_generator
        self.order_generator = order_generator

    def execute(self,
                observation: History,
                history: History,
                history_view: Type[HistoryView],
                *args, **kwargs) -> History:
        input_schema = history_view.history_schema(history.history_schema)
        signal_schema = self.signal_generator.output_schema(input_schema)
        def _generate(data):
            return self.signal_generator.generate(data, input_schema)

        signals: History = history_view.apply(history, _generate, signal_schema)
        orders = self.order_generator.generate(signals)
        return orders.loc(index=observation.data.index)

    def output_schema(self, history_schema: HistorySchema):
        signal_schema = self.signal_generator.output_schema(history_schema)
        order_schema = HistorySchema(signal_schema.index.copy(), {key: Order for key in signal_schema.column_names})
        return order_schema
