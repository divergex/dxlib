from typing import Type, Union

from dxlib import History
from .history_view import HistoryView


class Executor:
    def __init__(self, strategy):
        self.strategy = strategy

    def run(self, origin: History, history_view: Union[Type[HistoryView], HistoryView]):
        observer = history_view.iter(origin)

        if (observation := next(observer, None)) is None:
            return History(history_schema=self.strategy.output_schema(origin))

        history = observation.copy()
        result = History(history_schema=self.strategy.output_schema(observation)).concat(
            self.strategy.execute(observation, history, history_view)
        )

        for observation in observer:
            history.concat(observation)
            res = self.strategy.execute(observation, history, history_view)
            result.concat(
                res
            )
        return result
