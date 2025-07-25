import numpy as np
import pandas as pd

from dxlib import Strategy, History, Portfolio
from dxlib.core.portfolio import PortfolioHistory


class PortfolioStrategy(Strategy):
    def __init__(self, optimizer):
        self.optimizer = optimizer

    def output_schema(self, history: History):
        pass

    def execute(self,
                observation: History,
                history: History,
                history_view,
                *args, **kwargs) -> History:
        """
        Receives a history.py of inputs, as well as the latest data point, and returns a history.py of outputs.

        Args:
        """
        trading_days = 252
        returns = history.get(columns=["close"]).apply({"instruments": lambda x: np.log(x / x.shift(1))})
        expected_returns: pd.DataFrame = returns.apply({"instruments": lambda x: x.mean() * trading_days / 12}).data
        covariance_returns = returns.op(lambda x: x.unstack("instruments").cov())

        weights, _ =  self.optimizer.optim(expected_returns.to_numpy(), covariance_returns.to_numpy(), gamma := 5e-2)

        prices = history.get(index={"date": [returns.index("date")[0]]}).data.reset_index("date")["close"]
        target = Portfolio({
            expected_returns.index[i]: weights[i] for i in range(len(expected_returns))
        })

        return PortfolioHistory().insert(observation.data.index[0], target)
