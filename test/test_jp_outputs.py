import pytest
from src.jp_imports.data_process import DataTrade
from polars.testing import assert_frame_equal
import polars as pl
import os

@pytest.fixture(scope="module")
def setup_database():
    d = DataTrade("sqlite:///test.sqlite", dev=True)
    d.insert_int_jp("test/test_inserts/jp_data_sample.parquet", "data/external/code_agr.json")
    d.insert_int_org("test/test_inserts/org_data_sample.parquet")
    yield d  # Yield the DataTrade instance for use in tests
    os.remove("test.sqlite")

@pytest.mark.parametrize("time, types, ag", [
    ("yearly", "total", True),
    ("yearly", "total", False),
    ("yearly", "naics", True),
    ("yearly", "naics", False),
    ("yearly", "hts", True),
    ("yearly", "hts", False),
    ("fiscal", "total", True),
    ("fiscal", "total", False),
    ("fiscal", "naics", True),
    ("fiscal", "naics", False),
    ("fiscal", "hts", True),
    ("fiscal", "hts", False),
    ("qrt", "total", True),
    ("qrt", "total", False),
    ("qrt", "naics", True),
    ("qrt", "naics", False),
    ("qrt", "hts", True),
    ("qrt", "hts", False),
    ("monthly", "total", True),
    ("monthly", "total", False),
    ("monthly", "naics", True),
    ("monthly", "naics", False),
    ("monthly", "hts", True),
    ("monthly", "hts", False),
])
def test_jp_results(setup_database, time, types, ag):
    d = setup_database  # Access the DataTrade instance from the fixture
    df1 = d.process_int_jp(agg=time, types=types, agr=ag).to_polars()
    df2 = pl.read_parquet(f"test/test_outputs/jp_results_{time}_{types}_{ag}.parquet")

    assert_frame_equal(df1, df2, check_dtypes=False)
