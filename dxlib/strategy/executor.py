from typing import Type, Union, Tuple, Callable

from dxlib.core.portfolio import PortfolioHistory
from dxlib.history import History, HistoryView
from .strategy import Strategy
from ..interfaces import TradingInterface


class Executor:
    def __init__(self, strategy: Strategy, interface: TradingInterface, context_fn: Callable = None):
        self.strategy = strategy
        self.interface = interface

        if context_fn is not None:
            self.context_fn = context_fn
            self._execute = self._execute_context
        else:
            self._execute = self._execute_contextless

    def _execute_context(self, observation, history, history_view: HistoryView):
        history.concat(observation)
        args, kwargs = self.context_fn(observation, history, history_view)
        orders = self.strategy.execute(observation, history, history_view, *args, **kwargs)
        self.interface.send(orders.data['order'].values)
        return orders

    def _execute_contextless(self, observation, history, history_view: HistoryView):
        history.concat(observation)
        orders = self.strategy.execute(observation, history, history_view)
        self.interface.send(orders.data.values.flatten())
        return orders

    @property
    def market(self):
        return self.interface.market_interface

    @property
    def account(self):
        return self.interface.account_interface

    def run(self,
            history_view: Union[Type[HistoryView], HistoryView],
            history: History = None,
            ) -> Tuple[History, PortfolioHistory | None]:
        observer = self.market.subscribe(history_view)
        output_schema = self.strategy.output_schema(history_view.history_schema(self.market.history_schema()))
        result = History(output_schema)
        portfolio = PortfolioHistory(result.history_schema.copy().index)

        if history is None:
            if (observation := next(observer, None)) is None:
                return History(history_schema=output_schema), None
            history = observation.copy()

            if observation is not None:
                result = result.concat(
                    self._execute(observation, history, history_view)
                )
                portfolio.update(observation.data.index, self.account.portfolio())

        for observation in observer:
            result.concat(
                self._execute(observation, history, history_view)
            )
            portfolio.update(observation.data.index, self.account.portfolio())

        return result, portfolio
