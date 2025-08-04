from typing import Iterator

from dxlib.core import Instrument
from dxlib.history import History, HistorySchema
from .simulator import Simulator
from ..order_book import OrderBook


class LOBSimulator(Simulator):
    def __init__(self, **kwargs):
        self.lob = OrderBook()
    
    
    
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