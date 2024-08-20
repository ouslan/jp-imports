from src.import_export_tools.data_pull import DataPull

def main() -> None:
    # DataProcess(saving_dir="data/", agriculture=True, debug=True)
    DataPull(saving_dir="data/", debug=True)

if __name__ == "__main__":
    main()
