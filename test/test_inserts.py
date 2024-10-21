import pytest
from src.jp_imports.data_process import DataTrade
from src.jp_imports.data_pull import DataPull
from polars.testing import assert_frame_equal
import polars as pl
import ibis
import os

@pytest.fixture(scope="module")
def setup_database():
    d = DataPull("sqlite:///test.sqlite", dev=True)
    d.insert_int_jp("test/test_inserts/jp_data_sample.parquet", "data/external/code_agr.json")
    d.insert_int_org("test/test_inserts/org_data_sample.parquet")
    yield
    os.remove("test.sqlite")

def test_jp_import(setup_database):
    conn = ibis.sqlite.connect("test.sqlite")
    df1 = conn.table("jptradedata").to_polars()
    df2 = pl.read_parquet("test/test_inserts/test_jp_data.parquet")
    
    assert_frame_equal(df1, df2, check_dtypes=False)

def test_org_import(setup_database):
    conn = ibis.sqlite.connect("test.sqlite")
    df1 = conn.table("inttradedata").to_polars()
    df2 = pl.read_parquet("test/test_inserts/test_org_data.parquet")

    assert_frame_equal(df1, df2, check_dtypes=False)