from dotenv import load_dotenv
from tqdm import tqdm
import polars as pl
import requests
import zipfile
import urllib3
import os

load_dotenv()

class DataPull:
    """
    This class pulls data from the CENSUS and the Puerto Rico Institute of Statistics

    """

    def __init__(self, saving_dir:str) -> None:
        """
        Parameters
        ----------
        saving_dir : str
            The directory where the data will be saved.
        Returns
        -------
        None
        """

        self.saving_dir = saving_dir

        if not os.path.exists(self.saving_dir + "external/code_classification.json"):
            self.pull_file(url="https://raw.githubusercontent.com/ouslan/jp-imports/main/data/external/code_classification.json", filename=(self.saving_dir + "external/code_classification.json"))
        if not os.path.exists(self.saving_dir + "external/code_agr.json"):
            self.pull_file(url="https://raw.githubusercontent.com/ouslan/jp-imports/main/data/external/code_agr.json", filename=(self.saving_dir + "external/code_agr.json"))

        # Check if the saving directory exists
        if not os.path.exists(self.saving_dir + "raw"):
            os.makedirs(self.saving_dir + "raw")
        if not os.path.exists(self.saving_dir + "processed"):
            os.makedirs(self.saving_dir + "processed")
        if not os.path.exists(self.saving_dir + "external"):
            os.makedirs(self.saving_dir + "external")

    def pull_int_org(self) -> None:
        """
        Pulls data from the Puerto Rico Institute of Statistics. Saves them in the 
            raw directory as a parquet file.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        self.pull_file(url="http://www.estadisticas.gobierno.pr/iepr/LinkClick.aspx?fileticket=JVyYmIHqbqc%3d&tabid=284&mid=244930", filename=(self.saving_dir + "raw/tmp.zip"))
        # Extract the zip file
        with zipfile.ZipFile(self.saving_dir + "raw/tmp.zip", "r") as zip_ref:
            zip_ref.extractall(f"{self.saving_dir}raw/")

        # Extract additional zip files
        additional_files = ["EXPORT_HTS10_ALL.zip", "IMPORT_HTS10_ALL.zip"]
        for additional_file in additional_files:
            additional_file_path = os.path.join(f"{self.saving_dir}raw/{additional_file}")
            with zipfile.ZipFile(additional_file_path, "r") as zip_ref:
                zip_ref.extractall(os.path.join(f"{self.saving_dir}raw/"))

        imports = pl.read_csv(self.saving_dir + "raw/IMPORT_HTS10_ALL.csv", ignore_errors=True)
        exports = pl.read_csv(self.saving_dir + "raw/EXPORT_HTS10_ALL.csv", ignore_errors=True)
        df = pl.concat([imports, exports], how="vertical")

        for file in os.listdir(self.saving_dir + "raw/"):
            if not file.endswith(".parquet"):
                os.remove(self.saving_dir + "raw/" + file)

        df.write_parquet(self.saving_dir + "raw/int_instance.parquet")

    def pull_int_jp(self) -> None:
        """
        Pulls data from the Puerto Rico Institute of Statistics used by the JP.
            Saved them in the raw directory as parquet files.

        Parameters
        ----------
        None 

        Returns
        -------
        None
        """
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        url = "https://datos.estadisticas.pr/dataset/92d740af-97e4-4cb3-a990-2f4d4fa05324/resource/b4d10e3d-0924-498c-9c0d-81f00c958ca6/download/ftrade_all_iepr.csv"
        self.pull_file(url=url, filename=(self.saving_dir + "raw/jp_instance.csv"), verify=False)
        jp_df = pl.read_csv(self.saving_dir + "raw/jp_instance.csv", ignore_errors=True)
        jp_df.write_parquet(self.saving_dir + "raw/jp_instance.parquet")
        os.remove(self.saving_dir + "raw/jp_instance.csv")

    def pull_census_hts(self, end_year:int, start_year:int, exports:bool, state:str) -> None:
        """
        Pulls HTS data from the Census and saves them in a parquet file.

        Parameters
        ----------
        end_year: int
            The last year to pull data from.
        start_year: int
            The first year to pull data from.
        exports: bool
            If True, pulls exports data. If False, pulls imports data.
        state: str
            The state to pull data from (e.g. "PR" for Puerto Rico).

        Returns
        -------
        None
        """

        empty_df = [
            pl.Series("date", dtype=pl.Datetime),
            pl.Series("census_value", dtype=pl.Int64),
            pl.Series("comm_level", dtype=pl.String),
            pl.Series("commodity", dtype=pl.String),
            pl.Series("country_name", dtype=pl.String),
            pl.Series("contry_code", dtype=pl.String),
        ]
        census_df = pl.DataFrame(empty_df)
        base_url = "https://api.census.gov/data/timeseries/"
        key = os.getenv("CENSUS_API_KEY")

        if exports:
            param = 'CTY_CODE,CTY_NAME,ALL_VAL_MO,COMM_LVL,E_COMMODITY'
            flow = "intltrade/exports/statehs"
            naming = {"CTY_CODE": "contry_code", "CTY_NAME": "country_name", "ALL_VAL_MO": "census_value", "COMM_LVL": "comm_level", "E_COMMODITY": "commodity"}
            saving_path = f"{self.saving_dir}/raw/census_hts_exports.parquet"
        else:
            param = 'CTY_CODE,CTY_NAME,GEN_VAL_MO,COMM_LVL,I_COMMODITY'
            flow = "intltrade/imports/statehs"
            naming = {"CTY_CODE": "contry_code", "CTY_NAME": "country_name", "GEN_VAL_MO": "census_value", "COMM_LVL": "comm_level", "I_COMMODITY": "commodity"}
            saving_path = f"{self.saving_dir}/raw/census_hts_imports.parquet"

        for year in range(start_year, end_year + 1):

            url = f"{base_url}{flow}?get={param}&STATE={state}&key={key}&time={year}"
            r = requests.get(url).json()
            df = pl.DataFrame(r)
            names = df.select(pl.col("column_0")).transpose()
            df = df.drop("column_0").transpose()
            df = df.rename(names.to_dicts().pop()).rename(naming)
            df = df.with_columns(date=(pl.col("time") + "-01").str.to_datetime("%Y-%m-%d"))
            df = df.select(pl.col("date", "census_value", "comm_level", "commodity", "country_name", "contry_code"))
            df = df.with_columns(pl.col("census_value").cast(pl.Int64))
            census_df = pl.concat([census_df, df], how="vertical")

        census_df.write_parquet(saving_path)

    def pull_census_naics(self, end_year:int, start_year:int, exports:bool, state:str) -> None:
        """
        Pulls NAICS data from the Census and saves them in a parquet file.

        Parameters
        ----------
        end_year: int
            The last year to pull data from.
        start_year: int
            The first year to pull data from.
        exports: bool
            If True, pulls exports data. If False, pulls imports data.
        state: str
            The state to pull data from (e.g. "PR" for Puerto Rico).

        Returns
        -------
        None
        """
        empty_df = [
            pl.Series("date", dtype=pl.Datetime),
            pl.Series("census_value", dtype=pl.Int64),
            pl.Series("comm_level", dtype=pl.String),
            pl.Series("naics_code", dtype=pl.String),
            pl.Series("country_name", dtype=pl.String),
            pl.Series("contry_code", dtype=pl.String),
        ]
        census_df = pl.DataFrame(empty_df)
        base_url = "https://api.census.gov/data/timeseries/"
        key = os.getenv("CENSUS_API_KEY")

        if exports:
            param = 'CTY_CODE,CTY_NAME,ALL_VAL_MO,COMM_LVL,NAICS'
            flow = "intltrade/exports/statenaics"
            naming = {"CTY_CODE": "contry_code", "CTY_NAME": "country_name", "ALL_VAL_MO": "census_value", "COMM_LVL": "comm_level", "NAICS": "naics_code"}
            saving_path = f"{self.saving_dir}/raw/census_naics_exports.parquet"
        else:
            param = 'CTY_CODE,CTY_NAME,GEN_VAL_MO,COMM_LVL,NAICS'
            flow = "intltrade/imports/statenaics"
            naming = {"CTY_CODE": "contry_code", "CTY_NAME": "country_name", "GEN_VAL_MO": "census_value", "COMM_LVL": "comm_level", "NAICS": "naics_code"}
            saving_path = f"{self.saving_dir}/raw/census_naics_imports.parquet"

        for year in range(start_year, end_year + 1):

            url = f"{base_url}{flow}?get={param}&STATE={state}&key={key}&time={year}"
            r = requests.get(url).json()
            df = pl.DataFrame(r)
            names = df.select(pl.col("column_0")).transpose()
            df = df.drop("column_0").transpose()
            df = df.rename(names.to_dicts().pop()).rename(naming)
            df = df.with_columns(date=(pl.col("time") + "-01").str.to_datetime("%Y-%m-%d"))
            df = df.select(pl.col("date", "census_value", "comm_level", "naics_code", "country_name", "contry_code"))
            df = df.with_columns(pl.col("census_value").cast(pl.Int64))
            census_df = pl.concat([census_df, df], how="vertical")

        census_df.write_parquet(saving_path)

    def pull_file(self, url:str, filename:str, verify:bool=True) -> None:
        """
        Pulls a file from a URL and saves it in the filename. Used by the class to pull external files.

        Parameters
        ----------
        url: str
            The URL to pull the file from.
        filename: str
            The filename to save the file to.
        verify: bool
            If True, verifies the SSL certificate. If False, does not verify the SSL certificate.

        Returns
        -------
        None
        """
        chunk_size = 10 * 1024 * 1024

        with requests.get(url, stream=True, verify=verify) as response:
            total_size = int(response.headers.get('content-length', 0))

            with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024, desc='Downloading') as bar:
                with open(filename, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            file.write(chunk)
                            bar.update(len(chunk))  # Update the progress bar with the size of the chunk
