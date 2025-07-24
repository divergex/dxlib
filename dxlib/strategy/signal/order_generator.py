from typing import List

import numpy as np
import pandas as pd

from dxlib import Portfolio
from dxlib.history import History, HistorySchema
from dxlib.market import OrderEngine, Order, Side


class OrderGenerator:
    def __init__(self, percent = 0.05):
        self.percent = percent

    def to_order(self, row):
        if row['signal'].has_side():
            return pd.Series({
                "order": OrderEngine.market.percent_of_equity(row['instruments'], self.percent, row['signal'].to_side()),
            })
        else:
            return pd.Series({"order": Order.none()})

    def generate(self, signals: History) -> History:
        assert "signal" in signals, "Missing signal column."
        if 'instruments' not in signals.columns and 'instruments' in signals.indices:
            signals['instruments'] = signals.index('instruments')
        assert "instruments" in signals, ("This OrderGenerator requires a instruments per signal. "
                                       "Try passing with `signals.reset_index('instruments')` if 'instruments' is in the index.")
        orders = signals.apply(self.to_order, axis=1, output_schema=HistorySchema(index=signals.history_schema.index.copy(), columns={"order": Order}))
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