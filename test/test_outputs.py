import pytest
from src.jp_imports.data_process import DataTrade
from src.jp_imports.data_pull import DataPull
from polars.testing import assert_frame_equal
import polars as pl
import ibis
import os

@pytest.fixture(scope="module")
def setup_database():
    d = DataTrade("sqlite:///test.sqlite", dev=True)
    yield
    os.remove("test.sqlite")

def test_d(setup_database):