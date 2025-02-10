from src.jp_imports.data_process import DataTrade


def main() -> None:
    d = DataTrade()
    print(d.process_price().execute())


if __name__ == "__main__":
    main()
