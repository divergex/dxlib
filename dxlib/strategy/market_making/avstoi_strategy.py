import numpy as np

from dxlib import Strategy, History


def get_spread(row):
    mid_price = (row['ask'] + row['bid']) / 2
    return mid_price - row['bid'], row['ask'] - mid_price


class AvellanedaStoikov(Strategy):
    def __init__(self, gamma: float = 1e-2, k: float = 1.5, horizon: float = 1.0):
        self.gamma = gamma
        self.k = k
        self.horizon = horizon  # e.g., in minutes or seconds
        assert gamma > 0 and k > 0

    def output_schema(self, history: History):
        raise NotImplementedError()

    def estimate_volatility(self, history: History, history_view, window: int = 50):
        closes = history_view.slice(history, size=window)['mid_price']
        log_returns = np.log(np.array(closes)[1:] / np.array(closes)[:-1])
        return np.std(log_returns)

    def execute(self, observation: History, history: History, history_view, *args, **kwargs) -> History:
        bid_half_spread, ask_half_spread = history_view.apply(observation, get_spread)
        bid = observation['bid']
        ask = observation['ask']
        mid_price = (bid + ask) / 2

        inventory = observation.get('inventory', 0)  # or passed in args

        # Assume `mid_price` column was precomputed in history
        volatility = self.estimate_volatility(history, history_view)

        # Reservation price
        r_t = mid_price - self.gamma * volatility**2 * self.horizon * inventory

        # Optimal spread (Δ*)
        delta = (1 / self.gamma) * np.log(1 + self.gamma / self.k)

        # Optimal quotes
        optimal_bid = r_t - delta
        optimal_ask = r_t + delta

        return History({
            "bid_quote": optimal_bid,
            "ask_quote": optimal_ask,
            "reservation_price": r_t,
            "spread": 2 * delta
        })
