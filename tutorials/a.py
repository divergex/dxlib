import datetime
from abc import ABC, abstractmethod
from typing import Type
import numba as nb
import numpy as np
import pandas as pd

from dxlib.interfaces.external.yfinance.yfinance import YFinance
from dxlib import History, Strategy, Cache, Benchmark, Signal

benchmark = Benchmark()


class SignalGenerator(ABC):
    @abstractmethod
    def generate(self, data: pd.DataFrame):
        pass

    @abstractmethod
    def output_schema(self, history: History):
        pass

class HistoryView:
    @staticmethod
    def len(history: History):
        indices = history.index(name="date")
        return len(indices.unique())

    @staticmethod
    def apply(history: History, function: callable):
        return history.get(columns=["close"], b=benchmark).apply({"security": function})

    @staticmethod
    @benchmark.track("HistoryView.get")
    def get(origin: History, idx):
        return origin.get({"date": [idx]}, ["close"], b=benchmark)

    @classmethod
    def iter(cls, origin: History):
        for idx in origin.index(name="date"):
            yield cls.get(origin, idx)

@nb.njit
def fast_rsi(values: np.ndarray, window: int):
    rsi = np.full(values.shape, np.nan, dtype=np.float64)
    for col in range(values.shape[1]):
        gains = np.zeros(window)
        losses = np.zeros(window)

        for i in range(1, window + 1):
            delta = values[i, col] - values[i - 1, col]
            gains[i - 1] = max(delta, 0)
            losses[i - 1] = max(-delta, 0)

        avg_gain = np.mean(gains)
        avg_loss = np.mean(losses)

        for i in range(window + 1, values.shape[0]):
            delta = values[i, col] - values[i - 1, col]
            gain = max(delta, 0)
            loss = max(-delta, 0)

            avg_gain = (avg_gain * (window - 1) + gain) / window
            avg_loss = (avg_loss * (window - 1) + loss) / window

            rs = avg_gain / avg_loss if avg_loss != 0 else np.nan
            rsi[i, col] = 1 - (1 / (1 + rs)) if rs == rs else np.nan

    return rsi


class Rsi(SignalGenerator):
    def __init__(self, window=14, lower=0.3, upper=0.7, reverting=True, period=None):
        self.window = window
        self.period = period or 1
        self.lower = lower
        self.upper = upper
        self.up = Signal.SELL if reverting else Signal.BUY
        self.down = Signal.BUY if reverting else Signal.SELL

        assert (0 <= lower <= 1) and (0 <= upper <= 1) and (lower < upper)

    @benchmark.track("RSI.generate")
    def generate(self, data: pd.DataFrame):
        score = self.score(data)
        conditions = [score < self.lower, score > self.upper]
        choices = [self.down, self.up]
        return pd.DataFrame(np.select(conditions, choices, default=Signal.HOLD), index=score.index, columns=score.columns)

    @benchmark.track("RSI.score")
    def _score(self, data: pd.DataFrame):
        # Filter for faster processing
        group = data.tail(self.period + self.window)

        # Calculate RSI based on the last 'period + window' entries
        delta = group.diff().dropna()

        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        avg_gain = gain.rolling(window=self.window, min_periods=1).mean()
        avg_loss = loss.rolling(window=self.window, min_periods=1).mean()

        # Avoid division by zero
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 1 - (1 / (1 + rs))

        return rsi.tail(self.period).fillna(self.upper)

    @benchmark.track("RSI.score_fast")
    def score(self, data: pd.DataFrame):
        group = data.tail(self.period + self.window).to_numpy(dtype=np.float64)
        rsi = fast_rsi(group, self.window)
        rsi = pd.DataFrame(rsi, index=data.tail(self.period + self.window).index, columns=data.columns)
        return rsi.tail(self.period).fillna(self.upper)

    @classmethod
    def output_schema(cls, observation):
        return observation.history_schema.copy()

class SignalStrategy(Strategy):
    def __init__(self, signal: SignalGenerator):
        self.signal = signal

    @benchmark.track("Strategy.execute")
    def execute(self,
                observation: History,
                history: History,
                history_view: Type[HistoryView],
                *args, **kwargs) -> History:
        result: History = history_view.apply(history, self.signal.generate)
        return result.loc(index=observation.data.index)

    def output_schema(self, observation: History):
        return self.signal.output_schema(observation)

class Executor:
    def __init__(self, strategy):
        self.strategy = strategy

    def run(self, origin: History, history_view: Type[HistoryView]):
        observer = history_view.iter(origin)

        if (observation := next(observer, None)) is None:
            return History(history_schema=self.strategy.output_schema(origin))

        history = observation.copy()
        result = History(history_schema=self.strategy.output_schema(observation)).concat(
            self.strategy.execute(observation, history, history_view)
        )

        for observation in observer:
            history.concat(observation)
            result.concat(
                self.strategy.execute(observation, history, history_view)
            )
        return result


def main():
    market_api = YFinance()
    cache = Cache(".dx")
    storage = "market_data"

    symbols = ["AAPL", "MSFT", "PETR4.SA", "BBAS3.SA"]
    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2024, 12, 31)
    history = cache.cached(storage, History, market_api.historical, symbols, start, end)

    print(history.head())

    # strategy = SignalStrategy(Rsi(period=HistoryView.len(history)))
    # print("Backtest")
    # print(strategy.execute(history, history, HistoryView))

    strategy = SignalStrategy(Rsi())
    executor = Executor(strategy)
    print("Executor")
    print(executor.run(history, HistoryView))

    benchmark.report()

if __name__ == "__main__":
    main()
