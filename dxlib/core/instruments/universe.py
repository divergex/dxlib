from typing import Dict, List

import pandas as pd


class BaseFilter:
    def apply(self, data: Dict[str, pd.DataFrame]) -> List[str]:
        raise NotImplementedError("Each filter must implement the apply method.")


class VolumeFilter(BaseFilter):
    def __init__(self, min_volume: float, column: str = "volume"):
        self.min_volume = min_volume
        self.column = column

    def apply(self, data: Dict[str, pd.DataFrame]) -> List[str]:
        return [
            ticker for ticker, df in data.items()
            if df[self.column].mean() >= self.min_volume
        ]


class VolatilityFilter(BaseFilter):
    def __init__(self, min_volatility: float, column: str = "close"):
        self.min_volatility = min_volatility
        self.column = column

    def apply(self, data: Dict[str, pd.DataFrame]) -> List[str]:
        return [
            ticker for ticker, df in data.items()
            if df[self.column].pct_change().std() >= self.min_volatility
        ]


class PriceFilter(BaseFilter):
    def __init__(self, min_price: float, column: str = "close"):
        self.min_price = min_price
        self.column = column

    def apply(self, data: dict[str, pd.DataFrame]) -> list[str]:
        return [
            ticker for ticker, df in data.items()
            if df[self.column].mean() >= self.min_price
        ]


class FilterPipeline:
    def __init__(self, filters: list[BaseFilter]):
        self.filters = filters

    def apply(self, data: Dict[str, pd.DataFrame]) -> List[str]:
        passing_sets = [set(f.apply(data)) for f in self.filters]
        return list(set.intersection(*passing_sets)) if passing_sets else []


class Universe:
    def __init__(self, data):
        self.data = data

    def select(self, pipeline: FilterPipeline) -> List[str]:
        return pipeline.apply(self.data)
