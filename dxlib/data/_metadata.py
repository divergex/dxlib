"""
Asset metadata manager.

Stores asset metadata in a Parquet table and exposes fast filtering/universe selection APIs.
Maintains a mapping between symbols and their data presence.
"""
from __future__ import annotations

import os
from typing import Iterable, Optional, List, Any

import polars as pl


class AssetMetadata:
    FILENAME = "assets.parquet"

    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)
        self.path = os.path.join(self.storage_dir, self.FILENAME)

    def upsert(self, df: pl.DataFrame):
        if "symbol" not in df.columns:
            raise ValueError("metadata must include 'symbol' column")
        if not os.path.exists(self.path):
            df.write_parquet(self.path)
            return
        existing = pl.read_parquet(self.path)
        others = existing.join(df, on="symbol", how="anti")
        merged = pl.concat([others, df], how="vertical")
        merged.write_parquet(self.path)

    def query(self, symbols: Optional[Iterable[str]] = None, filters: Optional[List[tuple]] = None,
              columns: Optional[List[str]] = None):
        if not os.path.exists(self.path):
            return pl.DataFrame()
        df = pl.read_parquet(self.path)
        if symbols is not None:
            df = df.filter(pl.col("symbol").is_in(list(symbols)))
        if filters:
            for col, op, val in filters:
                if op == '==':
                    df = df.filter(pl.col(col) == val)
                elif op == '>':
                    df = df.filter(pl.col(col) > val)
                elif op == '<':
                    df = df.filter(pl.col(col) < val)
                elif op == 'in':
                    df = df.filter(pl.col(col).is_in(list(val)))
                else:
                    raise ValueError("Unsupported op: %s" % op)
        if columns:
            df = df.select(columns)
        return df

    def list_symbols(self):
        df = self.query()
        if df.is_empty():
            return []
        return df.get_column("symbol").to_list()
