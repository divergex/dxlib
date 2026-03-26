import os
import tempfile
from datetime import date

import polars as pl
import importlib.util
import pathlib

# Import modules by file location to avoid package-level side-effects when importing the whole
# `dxlib` package (which pulls optional external dependencies in `interfaces`).
ROOT = pathlib.Path(__file__).resolve().parents[1]
ss_path = ROOT / "dxlib" / "data" / "structured_storage.py"
meta_path = ROOT / "dxlib" / "data" / "metadata.py"

spec = importlib.util.spec_from_file_location("dxlib.data.structured_storage", ss_path)
ss_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ss_mod)

spec2 = importlib.util.spec_from_file_location("dxlib.data.metadata", meta_path)
meta_mod = importlib.util.module_from_spec(spec2)
spec2.loader.exec_module(meta_mod)

StructuredStorage = ss_mod.StructuredStorage
example_timeseries_schema = ss_mod.example_timeseries_schema
AssetMetadata = meta_mod.AssetMetadata


def test_append_and_query_timeseries(tmp_path):
    d = tmp_path / "storage"
    s = StructuredStorage(str(d))
    df = example_timeseries_schema()
    s.append_timeseries(df)
    out = s.query_timeseries()
    assert not out.is_empty()
    assert out.shape[0] == 1


def test_asset_metadata_upsert_and_query(tmp_path):
    d = tmp_path / "storage"
    m = AssetMetadata(str(d))
    meta = pl.DataFrame({"symbol": ["A"], "market_cap": [1_500_000_000], "avg_volume": [2_000_000]})
    m.upsert(meta)
    res = m.query(filters=[("market_cap", ">", 1_000_000_000)])
    assert not res.is_empty()
    assert res.select("symbol").get_column("symbol").to_list() == ["A"]
