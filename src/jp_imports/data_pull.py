from urllib.request import urlretrieve
from urllib.error import URLError
from pathlib import Path
import zipfile
import os

class DataPull:

    def __init__(self, saving_dir: str, debug: bool=False) -> None:
        self.saving_dir = Path(saving_dir)
        self.debug = debug

        if not os.path.exists(saving_dir / 'raw'):
            os.makedirs(saving_dir / 'raw')
        if not os.path.exists(saving_dir / 'processed'):
            os.makedirs(saving_dir / 'processed')

        self.pull_imp_exp()

    def pull_imp_exp(self):

        raw_folder_path = self.saving_dir / "raw"
        processed_folder_path = self.saving_dir / "processed"
        pull_file_save_path = self.saving_dir / "raw" / "tmp.zip"
        self.pull_file(url="http://www.estadisticas.gobierno.pr/iepr/LinkClick.aspx?fileticket=JVyYmIHqbqc%3d&tabid=284&mid=244930", filename=pull_file_save_path)

        # Extract the zip file
        with zipfile.ZipFile(pull_file_save_path, "r") as zip_ref:
            zip_ref.extractall(raw_folder_path)

        # Extract additional zip files
        additional_files = ["EXPORT_HTS10_ALL.zip", "IMPORT_HTS10_ALL.zip"]
        for additional_file in additional_files:
            additional_file_path = raw_folder_path / additional_file
            with zipfile.ZipFile(additional_file_path, "r") as zip_ref:
                zip_ref.extractall(raw_folder_path)

        # Remove the zip files and rename the CSV files
        for file in os.listdir(raw_folder_path):
            file_path = raw_folder_path / file
            if file.endswith(".zip"):
                os.remove(file_path)
            elif file.startswith("EXPORT") and file.endswith(".csv"):
                os.rename(file_path, raw_folder_path / "export.csv")
            elif file.startswith("IMPORT") and file.endswith(".csv"):
                os.rename(file_path, raw_folder_path / "import.csv")
            else:
                continue

    def pull_file(self, url: str, filename: str) -> None:
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
