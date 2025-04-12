from abc import abstractmethod, ABC

from ..history import History, HistorySchema


class Strategy(ABC):
    @abstractmethod
    def execute(self,
                observation: History,
                history: History,
                history_view,
                *args, **kwargs) -> History:
        """
        Receives a history.py of inputs, as well as the latest data point, and returns a history.py of outputs.

        Args:
        """
        raise NotImplementedError

    def __call__(self, observation: History=None, history: History=None, *args, **kwargs) -> History:
        return self.execute(observation, history, *args, **kwargs)

    @abstractmethod
    def output_schema(self, history: History):
        pass