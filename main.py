from src.jp_imports.data_process import DataProcess
from src.jp_imports.data_pull import DataPull

def main() -> None:
    save_path = "data/"
    df = DataProcess(saving_dir=save_path, instance="jp_instetute", debug=True).process_int_jp(time="monthly", types="total")
    print(df)
    #DataPull(saving_dir=save_path, state_code="PR", instance="jp_instetute", debug=True)

if __name__ == "__main__":
    main()
