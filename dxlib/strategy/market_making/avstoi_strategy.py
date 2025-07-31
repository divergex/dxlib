from dataclasses import dataclass

import numpy as np

from dxlib import Strategy, History, Portfolio, StrategyContext, Instrument, HistoryView, HistorySchema
from dxlib.interfaces import TradingInterface


def get_spread(row):
    mid_price = (row['ask'] + row['bid']) / 2
    return mid_price - row['bid'], row['ask'] - mid_price


@dataclass(slots=True)
class PortfolioContext(StrategyContext):
    portfolio: Portfolio

    @classmethod
    def from_interface(cls, interface: TradingInterface):
        context = cls(
            portfolio=interface.account_interface.portfolio()
        )
        return context

    @classmethod
    def build(cls, interface: TradingInterface) -> "PortfolioContext":
        return cls(portfolio=interface.account_interface.portfolio())

    @classmethod
    def bind(cls, interface: TradingInterface):
        return lambda: cls.build(interface)


class AvellanedaStoikov(Strategy):
    def __init__(self, gamma: float = 1e-2, k: float = 1.5, horizon: float = 1.0):
        self.gamma = gamma
        self.k = k
        self.horizon = horizon  # e.g., in minutes or seconds
        assert gamma > 0 and k > 0

    def output_schema(self, schema: HistorySchema):
        return HistorySchema(
            index=schema.index.copy(),
            columns={
                "bid_quote": float, "ask_quote": float,
                "reservation_price": float, "spread": float,
            }
        )

    def estimate_volatility(self, history: History, history_view: HistoryView, window: int = 50):
        closes = history_view.slice(history, size=window)
        log_returns = np.log(np.array(closes)[1:] / np.array(closes)[:-1])
        return np.std(log_returns)

    def execute(self,
                observation: History,
                history: History,
                history_view: HistoryView,
                context: PortfolioContext = None,
                *args, **kwargs) -> History:
        bid, ask = history_view.apply(observation, get_spread)
        mid_price = (bid + ask) / 2
        instrument: Instrument = observation.index("instrument")[0]
        inventory = context.portfolio.get(instrument)

        volatility = self.estimate_volatility(history, history_view)
        r_t = mid_price - self.gamma * volatility ** 2 * self.horizon * inventory
        delta = (1 / self.gamma) * np.log(1 + self.gamma / self.k)

        optimal_bid = r_t - delta
        optimal_ask = r_t + delta

        return History({
            "bid_quote": optimal_bid,
            "ask_quote": optimal_ask,
            "reservation_price": r_t,
            "spread": 2 * delta
        })
