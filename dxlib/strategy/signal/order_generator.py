import pandas as pd

from dxlib.market import OrderEngine


class OrderGenerator:
    def __init__(self, percent = 0.05):
        self.percent = percent

    def to_order(self, row):
        return pd.Series({"order": OrderEngine.market.percent_of_equity(row['security'], self.percent, row['signal'])})

    def generate(self, signals: pd.DataFrame):
        assert "signal" in signals, "Missing signal column."
        assert "security" in signals, ("This OrderGenerator requires a security per signal. "
                                       "Try passing with `signals.reset_index('security')` if 'security' is in the index.")
        orders = signals.apply(self.to_order, axis=1)
        return orders
