from abc import ABC, abstractmethod
from enum import Enum

from .history import HistorySchema, History


class Signal(Enum):
    BUY = 1
    SELL = -1
    HOLD = 0


class Strategy(ABC):
    def __init__(self, output_schema: HistorySchema):
        self.output_schema = output_schema

    @abstractmethod
    def execute(self,
                observation: History,
                history: History,
                *args, **kwargs) -> History:
        """
        Receives a history.py of inputs, as well as the latest data point, and returns a history.py of outputs.

        Args:
        """
        raise NotImplementedError

    def __call__(self, history: History, observation: History, *args, **kwargs) -> History:
        result = self.execute(history, observation, *args, **kwargs)
        if not isinstance(result, History) or result.schema != self.output_schema:
            raise ValueError("The strategy must return a history.py with the output schema.")
        return result
