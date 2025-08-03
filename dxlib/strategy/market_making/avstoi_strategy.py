import warnings
from typing import Optional

import numpy as np
import pandas as pd

from dxlib.core import Instrument
from dxlib.history import History, HistorySchema, HistoryView
from dxlib.market import OrderEngine, Order
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
                "bid": Order,
                "ask": Order,
            }
        )

    def estimate_volatility(self, history: History, history_view: HistoryView, window: int = 50):
        closes = history_view.get(history, -min(history_view.len(history), window))
        log_returns = np.log(np.array(closes.data)[1:] / np.array(closes.data)[:-1])
        if len(log_returns) < window:
            return 0
        vol = np.std(log_returns)
        return float(np.nan_to_num(vol))

    def execute(self,
                observation: History,
                history: History,
                history_view: HistoryView,
                context: Optional[PortfolioContext] = None,
                *args, **kwargs) -> History:
        assert context is not None, ("AvellanedaStoikov strategy requires passing a PortfolioContext builder. "
                                     "Try passing to the Executor's context_fn.")

        output_schema = self.output_schema(history.history_schema)
        mid_price = observation["price"]
        instrument: Instrument = observation.index("instrument")[0]
        inventory = context.portfolio.get(instrument)

        with warnings.catch_warnings():
            warnings.filterwarnings('error')
            volatility = self.estimate_volatility(history, history_view)
            r_t = mid_price - self.gamma * volatility ** 2 * self.horizon * inventory
            delta = (1 / self.gamma) * np.log(1 + self.gamma / self.k)

            optimal_bid = r_t - delta
            optimal_ask = r_t + delta
        df = pd.DataFrame(
            {
                "bid": optimal_bid.apply(lambda v: OrderEngine.limit.bid(instrument, v)),
                "ask": optimal_ask.apply(lambda v: OrderEngine.limit.ask(instrument, v))
            }
        )

        return History(output_schema, df)
