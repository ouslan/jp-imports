from src.features.download import Download
from src.data.data_clean import DataCleaner

def main(data_path, save_path1, save_path2):
    Download().download_data()
    #DataCleaner().country_trade(data_path, save_path1)
    DataCleaner().hts_trade(data_path, save_path2)


if __name__ == "__main__":
    data_path = "data/raw/data.csv"
    save_path1 = "data/processed/country_data.csv"
    save_path2 = "data/processed/hts_data.csv"
    main(data_path, save_path1, save_path2)