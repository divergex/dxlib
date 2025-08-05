from typing import Iterator, List, Optional

import numpy as np
import pandas as pd

from dxlib.core import Instrument
from dxlib.history import History, HistorySchema
from dxlib.core.dynamics import GeometricBrownianMotion
from .simulator import Simulator


class MidpriceGBM(Simulator):
    def __init__(self,
                 assets: Optional[List[Instrument]] = None,
                 midprice: List[float] | float = 1.0,
                 process: Optional[GeometricBrownianMotion] = None,
                 *args, **kwargs) -> None:
        self.assets = [Instrument("USD")] if assets is None else assets
        if isinstance(midprice, float):
            self.starting_prices = np.array([midprice] * len(self.assets))
        else:
            self.starting_prices = np.array(midprice)
        self.process = GeometricBrownianMotion(*args, **kwargs) if process is None else process

    @staticmethod
    def output_schema() -> HistorySchema:
        return HistorySchema(
            index={"time": float, "instrument": Instrument},
            columns={"price": float},
        )

    def run(self, dt, T) -> Iterator[History]:
        for prices, t in self.process.simulate(self.starting_prices, dt, T, len(self.assets)):
            time_array = [t + dt] * len(self.assets)
            df = pd.DataFrame(
                {"price": prices},
                index=pd.MultiIndex.from_arrays(
                    [time_array, self.assets],
                    names=["time", "instrument"],
                ),
            )
            yield History(self.output_schema(), df)
        return None


if __name__ == "__main__":
    midprice = 100.0
    simulator = MidpriceGBM(midprice=midprice, mean=0, std=1)

    print("\n\n".join(str(history.data) for history in simulator.run(10)))
