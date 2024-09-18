from abc import ABC, abstractmethod

from .history import HistorySchema, History


class Strategy(ABC):
    def __init__(self, output_schema: HistorySchema):
        self.output_schema = output_schema

    @abstractmethod
    def execute(self,
                history: History,
                observation: History,
                *args, **kwargs) -> History:
        """
        Receives a history of inputs, as well as the latest data point, and returns a history of outputs.

        Args:
        """
        raise NotImplementedError

    def __call__(self, history: History, observation: History, *args, **kwargs) -> History:
        result = self.execute(history, observation, *args, **kwargs)
        if not isinstance(result, History) or result.schema != self.output_schema:
            raise ValueError("The strategy must return a history with the output schema.")
        return result
