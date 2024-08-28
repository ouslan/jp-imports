#from src.jp_imports.data_process import DataProcess
from src.jp_imports.data_pull import DataPull

def main() -> None:
    save_path = "data/"
    #DataProcess(saving_dir=save_path, agriculture=True, debug=True)
    DataPull(saving_dir=save_path, state_code="PR", instance="jp_instetute", debug=True)

if __name__ == "__main__":
    main()
