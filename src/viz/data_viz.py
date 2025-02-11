from ..data.data_process import DataTrade


class DataViz(DataTrade):
    def __init__(
        self, saving_dir: str = "data/", database_url: str = "duckdb:///data.ddb"
    ):
        super().__init__(saving_dir, database_url)

    def gen_pie_chart(self, agg):
        self.process_int_jp(types="country", agg=agg, time=)
