from typing import Iterator, List

import numpy as np
import pandas as pd

from dxlib import History, HistorySchema, Instrument
from dxlib.core.dynamics import GeometricBrownianMotion


class Simulator:
    pass


class MidpriceGBM(Simulator):
    def __init__(self,
                 assets: List[Instrument] = None,
                 midprice: List[float] | float = 100.0,
                 increment=1,
                 process: GeometricBrownianMotion = None,
                 *args, **kwargs) -> None:
        self.assets = [Instrument("USD")] if assets is None else assets
        if isinstance(midprice, float):
            self.starting_prices = np.array([midprice] * len(self.assets))
        else:
            self.starting_prices = np.array(midprice)
        self.increment = increment

        self.process = GeometricBrownianMotion(*args, **kwargs) if process is None else process

    @staticmethod
    def output_schema() -> HistorySchema:
        return HistorySchema(
            index={"time": float, "instrument": Instrument},
            columns={"price": float},
        )

    def run(self, T=None) -> Iterator[History]:
        t = 0
        prices = self.starting_prices
        while T is None or t < T:
            time_array = [t + self.increment] * len(self.assets)
            df = pd.DataFrame(
                {"price": prices},
                index=pd.MultiIndex.from_arrays(
                    [time_array, self.assets],
                    names=["time", "instrument"],
                ),
            )
            yield History(self.output_schema(), df)
            prices = self.process.sample(prices, self.increment, len(self.assets))
            t += self.increment


if __name__ == "__main__":
    midprice = 100.0
    simulator = MidpriceGBM(midprice=midprice, mean=0, std=1 / midprice)

    print("\n\n".join(str(history.data) for history in simulator.run(10)))
