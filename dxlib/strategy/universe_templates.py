"""
Predefined universe templates for repeated strategy testing.

Each template is a callable that returns a Pipeline and optional metadata filters.
"""
from __future__ import annotations

from typing import Callable, Tuple, List
import polars as pl

from .filtering import Pipeline, LiquidityFilter, MarketCapFilter, SectorFilter, TopNFilter


def global_equities_template() -> Pipeline:
    p = Pipeline()
    # Example thresholds (these can be parameterized)
    p.add(LiquidityFilter(min_avg_volume=1_000_000, volume_col="avg_volume"))
    p.add(MarketCapFilter(min_market_cap=1_000_000_000))
    return p


def brazil_bonds_template() -> Pipeline:
    p = Pipeline()
    # For bonds, liquidity and region/asset class tags are important
    p.add(SectorFilter(sectors=["Fixed Income"]))
    # note: filter may be region-aware through metadata
    return p


def top_momentum_equities(n: int = 1000) -> Pipeline:
    p = Pipeline()
    p.add(LiquidityFilter(min_avg_volume=500_000, volume_col="avg_volume"))
    p.add(TopNFilter(n=n, sort_by="momentum", ascending=False))
    return p
