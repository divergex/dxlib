from typing import Type, Union

from dxlib import History
from dxlib.history.history_view import HistoryView
from ..core.portfolio import PortfolioHistory
from ..interfaces import TradingInterface


class Executor:
    def __init__(self, strategy, interface: TradingInterface):
        self.strategy = strategy
        self.interface = interface

    def _execute(self, observation, history, history_view: HistoryView):
        history.concat(observation)
        orders = self.strategy.execute(observation, history, history_view)
        self.interface.send(orders.data['order'].values)
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
            ):
        observer = self.market.subscribe(history_view)
        output_schema = self.strategy.output_schema(self.market.history_schema())
        observation = None

        if history is None:
            if (observation := next(observer, None)) is None:
                return History(history_schema=output_schema)
            history = observation.copy()
        result = History(output_schema)

        if observation is not None:
            result = result.concat(
                self._execute(observation, history, history_view)
            )
        portfolio = PortfolioHistory(result.history_schema.copy().index)
        portfolio.insert(self.account.portfolio(), observation.data.index)

        for observation in observer:
            result.concat(
                self._execute(observation, history, history_view)
            )
            portfolio.insert(self.account.portfolio(), observation.data.index)

        return result, portfolio
