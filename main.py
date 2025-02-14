from src.data.data_process import DataTrade


def main() -> None:
    d = DataTrade()
    print(d.process_int_jp(level="naics", time_frame="yearly").execute())


if __name__ == "__main__":
    main()
