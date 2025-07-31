from typing import List

import pandas as pd

from dxlib.core import Portfolio
from dxlib.history import History, HistorySchema
from dxlib.market import OrderEngine, Order, Side


class OrderGenerator:
    def __init__(self, percent=0.05):
        self.percent = percent

    def to_order(self, row):
        return row.drop("instruments").map(
            lambda v: None if v.to_side() is Side.NONE
            else OrderEngine.market.percent_of_equity(row["instruments"], self.percent, v.to_side())
        )

    def generate(self, signals: History) -> History:
        columns = signals.columns
        if 'instruments' not in signals.columns and 'instruments' in signals.indices:
            signals['instruments'] = signals.index('instruments')
        assert "instruments" in signals, ("This OrderGenerator requires a instruments per signal. "
                                          "Try passing with `signals.reset_index('instruments')` if 'instruments' is in the index.")
        orders = signals.apply([(self.to_order, (), {"axis":1}), (lambda x: x.dropna(),)],
                               output_schema=HistorySchema(
                                   index=signals.history_schema.index.copy(),
                                   columns={key: Order for key in columns},
                               ))
        return orders

    def from_target(self, current: Portfolio, target: Portfolio) -> List[Order]:
        orders = []
        for security in target.securities:
            quantity = target.get(security) - current.get(security)
            side = Side.signed(quantity)
            orders.append(OrderEngine.market.quantity(
                security, quantity=abs(quantity), side=side,
            )) if side != Side.NONE else None
        return orders
