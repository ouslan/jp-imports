from src.jp_imports.data_process import DataProcess

def main() -> None:
    save_path = "data/"
    DataProcess(saving_dir=save_path, agriculture=True, debug=True)

if __name__ == "__main__":
    main()
