"""
Filtering pipeline for universe selection.

Provides Filter base class and Pipeline which chains filters. Filters operate on an
asset metadata DataFrame and/or timeseries summary statistics.
"""
from __future__ import annotations

from typing import List, Iterable, Optional
import polars as pl


class Filter:
    """Base filter. Subclasses implement apply(metadata_df) -> symbols (iterable)"""

    def apply(self, metadata: pl.DataFrame) -> Iterable[str]:
        raise NotImplementedError()


class LiquidityFilter(Filter):
    def __init__(self, min_avg_volume: float, volume_col: str = "avg_volume"):
        self.min_avg_volume = min_avg_volume
        self.volume_col = volume_col

    def apply(self, metadata: pl.DataFrame):
        if self.volume_col not in metadata.columns:
            return []
        df = metadata.filter(pl.col(self.volume_col) >= self.min_avg_volume)
        return df.select("symbol").get_column("symbol").to_list()


class MarketCapFilter(Filter):
    def __init__(self, min_market_cap: float):
        self.min_market_cap = min_market_cap

    def apply(self, metadata: pl.DataFrame):
        if "market_cap" not in metadata.columns:
            return []
        df = metadata.filter(pl.col("market_cap") >= self.min_market_cap)
        return df.select("symbol").get_column("symbol").to_list()


class SectorFilter(Filter):
    def __init__(self, sectors: Iterable[str]):
        self.sectors = list(sectors)

    def apply(self, metadata: pl.DataFrame):
        if "sector" not in metadata.columns:
            return []
        df = metadata.filter(pl.col("sector").is_in(self.sectors))
        return df.select("symbol").get_column("symbol").to_list()


class TopNFilter(Filter):
    def __init__(self, n: int, sort_by: str, ascending: bool = False):
        self.n = n
        self.sort_by = sort_by
        self.ascending = ascending

    def apply(self, metadata: pl.DataFrame):
        if self.sort_by not in metadata.columns:
            return []
        df = metadata.sort(self.sort_by, reverse=(not self.ascending)).head(self.n)
        return df.select("symbol").get_column("symbol").to_list()


class Pipeline:
    def __init__(self, filters: Optional[List[Filter]] = None):
        self.filters = filters or []

    def add(self, f: Filter):
        self.filters.append(f)

    def run(self, metadata: pl.DataFrame) -> List[str]:
        # Start with all symbols
        current = metadata.select("symbol").get_column("symbol").to_list() if not metadata.is_empty() else []
        for f in self.filters:
            # apply filter to metadata but scoped to current selection
            if not current:
                return []
            scoped = metadata.filter(pl.col("symbol").is_in(current))
            result = list(f.apply(scoped))
            current = result
        return current
