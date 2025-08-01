import numpy as np
import pandas as pd

from dxlib.core import Instrument
from dxlib.history import History, HistorySchema, HistoryView
from .. import PortfolioContext, Strategy


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
        closes = history_view.get(history, -min(history_view.len(history), window))
        log_returns = np.log(np.array(closes.data)[1:] / np.array(closes.data)[:-1])
        vol = np.std(log_returns)
        return float(np.nan_to_num(vol))

    def execute(self,
                observation: History,
                history: History,
                history_view: HistoryView,
                context: PortfolioContext = None,
                *args, **kwargs) -> History:
        output_schema = self.output_schema(history.history_schema)
        mid_price = observation["price"]
        instrument: Instrument = observation.index("instrument")[0]
        inventory = context.portfolio.get(instrument)

        volatility = self.estimate_volatility(history, history_view)
        r_t = mid_price - self.gamma * volatility ** 2 * self.horizon * inventory
        delta = (1 / self.gamma) * np.log(1 + self.gamma / self.k)

        optimal_bid = r_t - delta
        optimal_ask = r_t + delta
        df = pd.DataFrame(
            {
                "bid_quote": optimal_bid,
                "ask_quote": optimal_ask,
                "reservation_price": r_t,
                "spread": 2 * delta
            }
        )

        return History(output_schema, df)
