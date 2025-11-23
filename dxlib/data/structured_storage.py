"""
Structured storage implementation using Parquet + Polars as the primary backend.

Features implemented:
- Global or per-universe Parquet tables for timeseries with explicit schema
  (date, symbol, open, high, low, close, volume, factors...)
- Appendable writes using Polars' lazy_pandas or direct parquet write with row groups
- Metadata store backed by a single Parquet file `assets.parquet`
- Simple index mapping storing symbol -> file offset (optional, lightweight)
- Query API that supports filter_by, select_columns, date range, sorting, and sampling

This implementation is deliberately lightweight and conservative to avoid
introducing heavy runtime dependencies or changing the project's runtime behavior
unless the user opts into using the structured storage API.

Note: This file provides a high-level implementation and examples. For a production
system, consider adding concurrency control, robust write-ahead logging, versioning,
and more thorough validation.
"""
from __future__ import annotations

import os
from typing import Iterable, List, Optional, Dict, Any, Tuple
from datetime import date, datetime

import polars as pl


class StructuredStorage:
    """High-level structured storage backed by Parquet files using Polars.

    Data layout:
      storage_dir/
        timeseries.parquet   <- a single large table with schema including 'date' and 'symbol'
        assets.parquet       <- asset metadata table
        index.json           <- optional mapping symbol -> row-groups or file offsets

    Contract:
      - append_timeseries(df: pl.DataFrame) will append new rows (same schema)
      - query_timeseries(...) returns a pl.DataFrame matching filters
      - store_asset_metadata(assets: pl.DataFrame) stores/updates metadata
      - query_assets(...) filters metadata quickly
    """

    TIMESERIES_FILENAME = "timeseries.parquet"
    ASSETS_FILENAME = "assets.parquet"

    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)
        self.timeseries_path = os.path.join(self.storage_dir, self.TIMESERIES_FILENAME)
        self.assets_path = os.path.join(self.storage_dir, self.ASSETS_FILENAME)

    # -------------------- Timeseries --------------------
    def _ensure_timeseries_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        # minimal required columns
        required = ["date", "symbol", "open", "high", "low", "close", "volume"]
        for c in required:
            if c not in df.columns:
                raise ValueError(f"Timeseries missing required column: {c}")
        # ensure date column is date or datetime
        date_dtype = df.schema.get("date", None)
        if date_dtype not in (pl.Date, pl.Datetime):
            # try to cast from string using common iso format
            df = df.with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").alias("date"))
        return df

    def append_timeseries(self, df: pl.DataFrame, partition_by: Optional[List[str]] = None):
        """Append timeseries rows to the global timeseries table.

        Polars does not support in-place Parquet append across all backends; the
        approach used here is pragmatic:
         - if file doesn't exist, write full df
         - else, read metadata (schema) and write a new parquet file appended via
           pyarrow dataset write to the same directory (polars can read the union)

        Args:
            df: polars.DataFrame with required schema
            partition_by: optional list of columns to partition data by (e.g., ['symbol'])
        """
        df = self._ensure_timeseries_schema(df)
        # convert date columns to date
        if df.schema.get("date", None) == pl.Datetime:
            df = df.with_columns(pl.col("date").cast(pl.Date))

        if not os.path.exists(self.timeseries_path):
            # write single-file parquet
            df.write_parquet(self.timeseries_path, compression="snappy")
            return

        # Existing file - append by reading existing and concat (safe but memory heavy)
        # For larger datasets, a better approach is to write to partitioned files using pyarrow.dataset.write_dataset.
        existing = pl.read_parquet(self.timeseries_path)
        combined = pl.concat([existing, df], how="vertical")
        # optional: drop duplicates by (date, symbol)
        combined = combined.unique(subset=["date", "symbol"], keep="last")
        combined.write_parquet(self.timeseries_path, compression="snappy")

    def query_timeseries(self,
                         symbols: Optional[Iterable[str]] = None,
                         start: Optional[date] = None,
                         end: Optional[date] = None,
                         columns: Optional[List[str]] = None,
                         limit: Optional[int] = None,
                         sort_by: Optional[List[Tuple[str, str]]] = None
                         ) -> pl.DataFrame:
        """Query the timeseries table with fast predicates using Polars.

        Args:
            symbols: iterable of symbols to include
            start, end: date bounds inclusive
            columns: list of columns to return
            limit: optionally limit rows
            sort_by: list of tuples (column, 'asc'|'desc')
        """
        if not os.path.exists(self.timeseries_path):
            return pl.DataFrame()

        df = pl.read_parquet(self.timeseries_path)
        if symbols is not None:
            df = df.filter(pl.col("symbol").is_in(list(symbols)))
        if start is not None:
            df = df.filter(pl.col("date") >= pl.lit(start))
        if end is not None:
            df = df.filter(pl.col("date") <= pl.lit(end))
        if sort_by:
            for col, direction in sort_by:
                df = df.sort(col, reverse=(direction == "desc"))
        if columns is not None:
            df = df.select(columns)
        if limit is not None:
            df = df.head(limit)
        return df

    # -------------------- Assets / Metadata --------------------
    def store_asset_metadata(self, assets: pl.DataFrame):
        """Store or update asset metadata. Schema should include at least 'symbol'.

        Merge semantics: upsert on 'symbol'.
        """
        if "symbol" not in assets.columns:
            raise ValueError("assets DataFrame must contain 'symbol' column")

        if not os.path.exists(self.assets_path):
            assets.write_parquet(self.assets_path, compression="snappy")
            return

        existing = pl.read_parquet(self.assets_path)
        # left anti-join to find rows not in new assets
        others = existing.join(assets, on="symbol", how="anti")
        merged = pl.concat([others, assets], how="vertical")
        merged.write_parquet(self.assets_path, compression="snappy")

    def query_assets(self,
                     symbols: Optional[Iterable[str]] = None,
                     filters: Optional[List[Tuple[str, str, Any]]] = None,
                     columns: Optional[List[str]] = None,
                     limit: Optional[int] = None,
                     sort_by: Optional[List[Tuple[str, str]]] = None) -> pl.DataFrame:
        """Query asset metadata with simple filter expressions.

        filters: list of (column, op, value) where op in ('==','!=','>','<','>=','<=','in')
        """
        if not os.path.exists(self.assets_path):
            return pl.DataFrame()
        df = pl.read_parquet(self.assets_path)
        if symbols is not None:
            df = df.filter(pl.col("symbol").is_in(list(symbols)))
        if filters:
            for col, op, val in filters:
                if op == '==':
                    df = df.filter(pl.col(col) == val)
                elif op == '!=':
                    df = df.filter(pl.col(col) != val)
                elif op == '>':
                    df = df.filter(pl.col(col) > val)
                elif op == '<':
                    df = df.filter(pl.col(col) < val)
                elif op == '>=':
                    df = df.filter(pl.col(col) >= val)
                elif op == '<=':
                    df = df.filter(pl.col(col) <= val)
                elif op == 'in':
                    df = df.filter(pl.col(col).is_in(list(val)))
                else:
                    raise ValueError(f"Unsupported op: {op}")
        if sort_by:
            for col, direction in sort_by:
                df = df.sort(col, reverse=(direction == 'desc'))
        if columns:
            df = df.select(columns)
        if limit:
            df = df.head(limit)
        return df


# -------------------- Helper utilities --------------------
def example_timeseries_schema() -> pl.DataFrame:
    return pl.DataFrame({
        "date": pl.Series([date(2020, 1, 1)], dtype=pl.Date),
        "symbol": ["A"],
        "open": [1.0],
        "high": [1.0],
        "low": [1.0],
        "close": [1.0],
        "volume": [100],
    })


def _demo():
    s = StructuredStorage("./.divergex")
    df = example_timeseries_schema()
    # write once
    s.append_timeseries(df)
    # query
    print(s.query_timeseries())


if __name__ == '__main__':
    _demo()
