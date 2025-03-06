from dxlib import History, HistorySchema
from ..strategy import Strategy


class SignalStrategy(Strategy):
    def __init__(self, output_schema: HistorySchema, signal):
        super().__init__(output_schema)
        self.signal = signal

    def execute(self,
                observation: History = None,
                history: History = None,
                *args, **kwargs) -> History:
        """
        Execute trading signals based on the RSI indicator.
        """
        result: History = self.signal.get_signals(history)
        index_value = observation.data.index
        index_name = observation.history_schema.index
        search_key = {name: [index_value[i]] for i, name in enumerate(index_name)}
        return result.get(index=search_key)  # Return only the signals for the current observation
