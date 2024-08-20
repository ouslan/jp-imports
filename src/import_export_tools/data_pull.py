from urllib.request import urlretrieve
from urllib.error import URLError
import polars as pl
import requests
from dotenv import load_dotenv
import zipfile
import os

load_dotenv()

class DataPull:

    def __init__(self, saving_dir:str, debug:bool=False) -> None:
        self.saving_dir = saving_dir
        self.debug = debug
        if not os.path.exists(self.saving_dir + "raw"):
            os.makedirs(self.saving_dir + "raw")
        if not os.path.exists(self.saving_dir + "processed"):
            os.makedirs(self.saving_dir + "processed")
        self.pull_imp_exp()
        self.pull_census(end_year=2023, start_year=2013, exports=True, state="PR", saving_dir=self.saving_dir)
        self.pull_census(end_year=2023, start_year=2013, exports=False, state="PR", saving_dir=self.saving_dir)

    def pull_imp_exp(self):

        if not os.path.exists(self.saving_dir + "raw/import.csv") and not os.path.exists(self.saving_dir + "raw/export.csv") or not os.path.exists(self.saving_dir + "processed/imp_exp.parquet"):
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

            # Remove the zip files and rename the CSV files
            for file in os.listdir(os.path.join("data", "raw")):
                file_path = os.path.join("data", "raw", file)
                if file.endswith(".zip"):
                    os.remove(file_path)
                elif file.startswith("EXPORT") and file.endswith(".csv"):
                    os.rename(file_path, os.path.join("data", "raw", "export.csv"))
                elif file.startswith("IMPORT") and file.endswith(".csv"):
                    os.rename(file_path, os.path.join("data", "raw", "import.csv"))
                else:
                    continue
        else:
            if self.debug:
                print("\033[0;36mNOTICE: \033[0m" + "Import and Export files already exist, skipping download")

    def pull_file(self, url:str, filename:str) -> None:
        if os.path.exists(filename):
            if self.debug:
                print("\033[0;36mNOTICE: \033[0m" + f"File {filename} already exists, skipping download")
        else:
            try:
                urlretrieve(url, filename)
                if self.debug:
                    print("\033[0;32mINFO: \033[0m" + f"Downloaded {filename}")
            except URLError:
                if self.debug:
                    print("\033[1;33mWARNING:  \033[0m" + f"Could not download {filename}")

    def pull_census(self, end_year:int, start_year:int, exports:bool, state:str, saving_dir:str) -> None:
        empty_df = [
            pl.Series("date", dtype=pl.Datetime),
            pl.Series("census_value", dtype=pl.Int64),
            pl.Series("comm_level", dtype=pl.String),
            pl.Series("commodity", dtype=pl.String),
            pl.Series("country_name", dtype=pl.String),
            pl.Series("contry_code", dtype=pl.String),
        ]
        census_df = pl.DataFrame(empty_df)
        if not os.path.exists(f"{saving_dir}/raw/census_{type}.parquet"):
            base_url = "https://api.census.gov/data/timeseries/"
            key = os.getenv("CENSUS_API_KEY")

            if exports:
                param = 'CTY_CODE,CTY_NAME,ALL_VAL_MO,COMM_LVL,E_COMMODITY'
                flow = "intltrade/exports/statehs"
                naming = {"CTY_CODE": "contry_code", "CTY_NAME": "country_name", "ALL_VAL_MO": "census_value", "COMM_LVL": "comm_level", "E_COMMODITY": "commodity"}
                saving_path = f"{saving_dir}/raw/census_exports.parquet"
            else:
                param = 'CTY_CODE,CTY_NAME,GEN_VAL_MO,COMM_LVL,I_COMMODITY'
                flow = "intltrade/imports/statehs"
                naming = {"CTY_CODE": "contry_code", "CTY_NAME": "country_name", "GEN_VAL_MO": "census_value", "COMM_LVL": "comm_level", "I_COMMODITY": "commodity"}
                saving_path = f"{saving_dir}/raw/census_imports.parquet"
 
            for year in range(start_year, end_year + 1):

                url = f"{base_url}{flow}?get={param}&STATE={state}&key={key}&time={year}"
                r = requests.get(url).json()
                df = pl.DataFrame(r)
                names = df.select(pl.col("column_0")).transpose()
                df = df.drop("column_0").transpose()
                df = df.rename(names.to_dicts().pop())
                df = df.rename(naming)
                df = df.with_columns(date=(pl.col("time") + "-01").str.to_datetime("%Y-%m-%d"))
                df = df.select(pl.col("date", "census_value", "comm_level", "commodity", "country_name", "contry_code"))
                df = df.with_columns(pl.col("census_value").cast(pl.Int64))
                census_df = pl.concat([census_df, df], how="vertical")
                if self.debug:
                    print("\033[0;32mINFO: \033[0m" + f"Downloaded {year} data")

            census_df.write_parquet(saving_path)
        else:
            if self.debug:
                print("\033[0;36mNOTICE: \033[0m" + "Census data already exists, skipping download")
