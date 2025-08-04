from typing import Tuple, Callable, Iterator, Optional

from dxlib.core.portfolio import PortfolioHistory
from dxlib.history import History, HistoryView, HistorySchema
from .strategy import Strategy
from ..interfaces import TradingInterface
from ..market import OrderTransaction


class Executor:
    def __init__(self, strategy: Strategy, interface: TradingInterface, context_fn: Optional[Callable] = None):
        self.strategy = strategy
        self.interface = interface

        if context_fn is not None:
            self.context_fn = context_fn
            self._execute = self._execute_context
        else:
            self._execute = self._execute_contextless

    def _execute_context(self,
                         observation: History,
                         history: History,
                         history_view: HistoryView
                         ) -> History:
        history.concat(observation)
        context = self.context_fn(observation, history, history_view)
        orders = self.strategy.execute(observation, history, history_view, context)
        self.interface.order.send(orders.data.values.flatten().tolist())
        transactions = self.interface.order.transactions()
        transactions = History(
            HistorySchema(
                orders.history_schema.index,
                {col: OrderTransaction for col in orders.history_schema.columns},
            ),
            orders.data.map(lambda order: transactions.get(order.uuid)).dropna()
        )
        return transactions

    def _execute_contextless(self,
                             observation: History,
                             history: History,
                             history_view: HistoryView
                             ) -> History:
        history.concat(observation)
        orders = self.strategy.execute(observation, history, history_view)
        self.interface.order.send(orders.data.values.flatten().tolist())
        transactions = self.interface.order.transactions()

    @property
    def market(self):
        return self.interface.market

    @property
    def account(self):
        return self.interface.account

    def output_schema(self, input_schema):
        return HistorySchema(
            input_schema.index,
            {col: OrderTransaction for col in input_schema.columns},
        )

    def run(self,
            history_view: HistoryView,
            observer: Iterator[History],
            history: Optional[History] = None,
            ) -> Tuple[History, PortfolioHistory]:
        input_schema = self.strategy.output_schema(history_view.history_schema(self.market.history_schema()))
        output_schema = self.output_schema(input_schema)
        result = History(output_schema)
        portfolio_history = PortfolioHistory(result.history_schema.copy().index)

        if history is None:
            if (observation := next(observer, None)) is None:
                return History(history_schema=output_schema), PortfolioHistory(output_schema.index.copy())
            history = observation.copy()
            result = result.concat(
                self._execute(observation, history, history_view)
            )
            portfolio_history.update(observation.data.index, self.account.portfolio())

        for observation in observer:
            result.concat(
                self._execute(observation, history, history_view)
            )
            portfolio_history.update(observation.data.index, self.account.portfolio())

        return result, portfolio_history
