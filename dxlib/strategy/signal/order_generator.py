import pandas as pd

from dxlib.history import History, HistorySchema
from dxlib.market import OrderEngine, Order, Side


class OrderGenerator:
    def __init__(self, percent = 0.05):
        self.percent = percent

    def to_order(self, row):
        if row['signal'].has_side():
            return pd.Series({
                "order": OrderEngine.market.percent_of_equity(row['security'], self.percent, row['signal'].to_side()),
            })
        else:
            return pd.Series({"order": Order.none()})

    def generate(self, signals: History) -> History:
        assert "signal" in signals, "Missing signal column."
        if 'security' not in signals.columns and 'security' in signals.indices:
            signals['security'] = signals.index('security')
        assert "security" in signals, ("This OrderGenerator requires a security per signal. "
                                       "Try passing with `signals.reset_index('security')` if 'security' is in the index.")
        orders = signals.apply(self.to_order, axis=1, output_schema=HistorySchema(index=signals.history_schema.index.copy(), columns={"order": Order}))
        return orders
