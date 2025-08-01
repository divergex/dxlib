from abc import abstractmethod, ABC

from dxlib.history import History, HistorySchema, HistoryView


class StrategyContext:
    pass


class Strategy(ABC):
    @abstractmethod
    def execute(self,
                observation: History,
                history: History,
                history_view: HistoryView,
                context: StrategyContext = None,
                *args, **kwargs) -> History:
        """
        Receives a history.py of inputs, as well as the latest data point, and returns a history.py of outputs.

        Args:
        """
        raise NotImplementedError

    def __call__(self, observation: History=None, history: History=None, history_view=None, context: StrategyContext=None, *args, **kwargs) -> History:
        return self.execute(observation, history, history_view, context, *args, **kwargs)

    @abstractmethod
    def output_schema(self, history: HistorySchema):
        pass