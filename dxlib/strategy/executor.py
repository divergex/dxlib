from typing import Type, Union, Iterator

from dxlib import History
from .history_view import HistoryView
from ..core.portfolio import PortfolioHistory
from ..interfaces import TradingInterface


class Executor:
    def __init__(self, strategy, interface: TradingInterface):
        self.strategy = strategy
        self.interface = interface

    def _execute(self, observation, history, history_view):
        history.concat(observation)
        orders = self.strategy.execute(observation, history, history_view)
        self.interface.send(orders)
        return orders

    def run(self,
            origin: History | Iterator[History],
            history_view: Union[Type[HistoryView], HistoryView],
            history: History = None,
            ):
        observer = history_view.iter(origin) if isinstance(origin, History) else origin

        observation = None

        if history is None:
            if (observation := next(observer, None)) is None:
                return History(history_schema=self.strategy.output_schema(origin))
            history = observation.copy()
        result = History(self.strategy.output_schema(origin))

        if observation is not None:
            result = result.concat(
                self.strategy.execute(observation, history, history_view)
            )
        portfolio = PortfolioHistory(result.history_schema.copy().index)

        for observation in observer:
            result.concat(
                self._execute(observation, history, history_view)
            )
            portfolio.concat(self.interface.portfolio())

        return result
