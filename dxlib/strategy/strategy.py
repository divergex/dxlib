from abc import abstractmethod, ABC

from ..history import History, HistorySchema


class Strategy(ABC):
    def __init__(self, output_schema: HistorySchema = None):
        self.output_schema = output_schema

    @abstractmethod
    def execute(self,
                observation: History=None,
                history: History=None,
                *args, **kwargs) -> History:
        """
        Receives a history.py of inputs, as well as the latest data point, and returns a history.py of outputs.

        Args:
        """
        raise NotImplementedError

    def __call__(self, observation: History=None, history: History=None, *args, **kwargs) -> History:
        return self.execute(observation, history, *args, **kwargs)
