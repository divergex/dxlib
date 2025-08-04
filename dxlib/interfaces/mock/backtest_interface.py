from typing import List, Optional, Dict

import pandas as pd

from dxlib.core import Portfolio, Instrument
from dxlib.history import History, HistorySchema, HistoryView
from dxlib.interfaces import TradingInterface, OrderInterface, AccountInterface, MarketInterface
from dxlib.market import Order, OrderEngine, OrderTransaction
from dxlib.market.simulators.fill_model import FillModelRegistry, ImmediateMarketFillModel


class BacktestOrderInterface(OrderInterface):
    def __init__(self, context: "BacktestInterface", fill_registry: FillModelRegistry):
        super().__init__()
        self.context = context
        self.fill_registry = fill_registry
        self._transactions = {}

    def send(self, orders: List[Order]) -> Dict[str, OrderTransaction]:
        self._transactions = {}
        transactions = {
            order.uuid: tx for order in orders
            if not order.is_none()
            for tx in (
                (
                    self.fill_registry.get(order.instrument)
                    .fill(order, self.context)
                    ,)
            )
            if tx is not None
        }
        if not transactions:
            return {}
        self.context.order_engine.record(self.context.portfolio, list(transactions.values()))
        self._transactions.update(transactions)
        return self._transactions
    
    def transactions(self) -> Dict[str, OrderTransaction]:
        return self._transactions

class BacktestAccountInterface(AccountInterface):
    def __init__(self, context: "BacktestInterface"):
        super().__init__()
        self.context = context

    def portfolio(self) -> Portfolio:
        return self.context.portfolio

    def equity(self, *args, **kwargs) -> float:
        portfolio = self.context.portfolio
        prices = self.context.market.quote(portfolio.securities)
        return portfolio.value(prices)


class BacktestMarketInterface(MarketInterface):
    prices: pd.Series

    def __init__(self, context: "BacktestInterface", base_security: Optional[Instrument] = None):
        super().__init__()
        self.context = context
        self.index = None
        self.observation = None
        self.base_security = base_security or Instrument("USD")
        self.history = History()

        self.prices = pd.Series({self.base_security: 1.0}, name="price", dtype="float")
        self.prices.index.name = "instrument"
        self.price_history = History()

    def quote(self, security: str | Instrument | List[Instrument] | List[str]) -> pd.Series:
        if isinstance(security, List) and len(security) > 0 and isinstance(security[0], str):
            instruments = [Instrument(sec) for sec in security]
        elif isinstance(security, (Instrument, str)):
            instruments = [Instrument(security) if isinstance(security, str) else security]
        else:
            instruments = security
        return self.prices.loc[instruments]

    def history_schema(self) -> HistorySchema:
        return self.context.history.history_schema.copy()

    def get_view(self):
        for observation in self.context.history_view.iter(self.context.history):
            self.history.concat(observation)
            prices, idx = self.context.history_view.price(observation)
            if not prices.empty:
                self.prices = self.prices.combine_first(prices)
                self.prices.update(prices)
                self.price_history.concat_data(self.prices, idx)
            yield observation

class BacktestInterface(TradingInterface):
    account: BacktestAccountInterface
    order: BacktestOrderInterface
    market: BacktestMarketInterface

    def __init__(self, 
                 history: History, 
                 history_view: HistoryView, 
                 portfolio: Optional[Portfolio] = None,
                 fill_registry: Optional[FillModelRegistry] = None,
                 ):
        self.history = history
        self.history_view = history_view
        self.portfolio = portfolio if portfolio is not None else Portfolio()
        self.order_engine = OrderEngine()
        fill_registry = fill_registry if fill_registry is not None else FillModelRegistry(ImmediateMarketFillModel())

        super().__init__(
            account=BacktestAccountInterface(self),
            order=BacktestOrderInterface(self, fill_registry),
            market=BacktestMarketInterface(self)
        )

    def iter(self):
        return self.market.get_view()
