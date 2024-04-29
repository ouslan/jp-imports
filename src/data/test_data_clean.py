import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from src.data.data_clean import DataCleaner
import os


@pytest.fixture
def mock_data_clean():

    datacleaner = DataCleaner()

    return datacleaner


def test_clean(mock_data_clean):
    df_export = mock_data_clean.clean(f"{os.getcwd()}/data/testing/export_test.pkl")
    expected_df_export = pd.DataFrame(
        {
            "date": [
                pd.to_datetime("2018-06-01"),
                pd.to_datetime("2013-01-01"),
                pd.to_datetime("2003-07-01"),
            ],
            "country": ["Malaysia", "Montserrat", "United States"],
            "value": [float(368964), float(2765), float(22960)],
        }
    )

    df_import = mock_data_clean.clean(f"{os.getcwd()}/data/testing/import_test.pkl")

    expected_df_import = pd.DataFrame(
        {
            "date": [
                pd.to_datetime("2020-08-01"),
                pd.to_datetime("2018-06-01"),
                pd.to_datetime("2002-07-01"),
            ],
            "country": ["Korea", "Malaysia", "United States"],
            "value": [float(59000), float(78058), float(40799)],
        }
    )

    assert_frame_equal(expected_df_export, df_export)
    assert_frame_equal(expected_df_import, df_import)


def test_net_value(mock_data_clean):
    input_exports = pd.DataFrame(
        {
            "date": [
                pd.to_datetime("2018-06-01"),
                pd.to_datetime("2013-01-01"),
                pd.to_datetime("2003-07-01"),
            ],
            "country": ["Malaysia", "Montserrat", "United States"],
            "value": [float(368964), float(2765), float(22960)],
        }
    )

    input_imports = pd.DataFrame(
        {
            "date": [
                pd.to_datetime("2020-08-01"),
                pd.to_datetime("2018-06-01"),
                pd.to_datetime("2002-07-01"),
            ],
            "country": ["Korea", "Malaysia", "United States"],
            "value": [float(59000), float(78058), float(40799)],
        }
    )

    saving_path = f"{os.getcwd()}/data/processed/net_value_test.pkl"

    df = mock_data_clean.net_value(input_imports, input_exports, saving_path)

    excepted_df = pd.DataFrame(
        {
            "date": [
                pd.to_datetime("2020-08-01"),
                pd.to_datetime("2018-06-01"),
                pd.to_datetime("2013-01-01"),
                pd.to_datetime("2002-07-01"),
                pd.to_datetime("2003-07-01"),
            ],
            "country": [
                "Korea",
                "Malaysia",
                "Montserrat",
                "United States",
                "United States",
            ],
            "exports": [float(0), float(368964), float(2765), float(0), float(22960)],
            "imports": [float(59000), float(78058), float(0), float(40799), float(0)],
            "net_value": [
                float(-59000),
                float(290906),
                float(2765),
                float(-40799),
                float(22960),
            ],
        }
    )

    assert_frame_equal(excepted_df, df)
