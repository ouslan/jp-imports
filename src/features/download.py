import os
from urllib.request import urlretrieve
import polars as pl
import pandas as pd
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

class Download:
    def download_data(self):
        url = "https://datos.estadisticas.pr/dataset/92d740af-97e4-4cb3-a990-2f4d4fa05324/resource/b4d10e3d-0924-498c-9c0d-81f00c958ca6/download/ftrade_all_iepr.csv"
        file_path = os.path.join(os.getcwd(), "data", "raw", "data.csv")

        if not os.path.exists(os.getcwd() + "/data/raw" + "/data.csv"):
            urlretrieve(url, file_path)

if __name__ == "__main__":
    Download().download_data()