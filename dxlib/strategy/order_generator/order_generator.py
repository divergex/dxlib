from typing import List

from dxlib.core import Portfolio
from dxlib.history import History, HistorySchema
from dxlib.market import OrderEngine, Order, Side


class OrderGenerator:
    def __init__(self, percent=0.05):
        self.percent = percent

    def to_order(self, row):
        return row.drop("instrument").map(
            lambda v: None if Side.from_signal(v) is Side.NONE
            else OrderEngine.market.percent_of_equity(row["instrument"], self.percent, Side.from_signal(v))
        )

    def generate(self, signals: History) -> History:
        columns = signals.columns
        if 'instrument' not in signals.columns and 'instrument' in signals.indices:
            signals['instrument'] = signals.index('instrument')
        assert "instrument" in signals, ("This OrderGenerator requires a instruments per signal. "
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
