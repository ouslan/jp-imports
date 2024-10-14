from src.jp_imports.data_process import DataProcess
from src.jp_imports.data_pull import DataPull
from dotenv import load_dotenv
import os

load_dotenv()

def main() -> None:
    d = DataProcess(str(os.environ.get("DATABASE_URL")))
    print(d.process_jp_base())
if __name__ == "__main__":
    main()
