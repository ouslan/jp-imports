from .data_pull import DataPull
import polars as pl
import json
import os

class DataProcess(DataPull):

    def __init__(self, saving_dir:str, instance:str, state_code:str="PR", debug:bool=False):
        self.saving_dir = saving_dir
        self.state_code = state_code
        self.debug = debug
        self.instance = instance

        super().__init__(saving_dir=self.saving_dir, state_code=self.state_code, instance=self.instance,  debug=self.debug)
        self.codes = json.load(open(self.saving_dir + "external/code_classification.json"))

    def process_int_jp(self, time:str, types:str, group:bool=False):
        switch = [time, types]
        if group:
            #return self.process_cat(switch=switch)
            print("Not implemented")
        else:
            return self.process_data(switch)


    def process_base(self) -> pl.DataFrame:
        df = pl.read_parquet(self.saving_dir + "raw/jp_instance.parquet")

        df = df.with_columns(conv_1=pl.when(pl.col("unit_1").str.to_lowercase() == "kg").then(pl.col("qty_1") * 1)
                                        .when(pl.col("unit_1").str.to_lowercase() == "l").then(pl.col("qty_1") * 1)
                                        .when(pl.col("unit_1").str.to_lowercase() == "doz").then(pl.col("qty_1") / 0.756)
                                        .when(pl.col("unit_1").str.to_lowercase() == "m3").then(pl.col("qty_1") * 1560)
                                        .when(pl.col("unit_2").str.to_lowercase() == "t").then(pl.col("qty_1") * 907.185)
                                        .when(pl.col("unit_1").str.to_lowercase() == "kts").then(pl.col("qty_1") * 1)
                                        .when(pl.col("unit_1").str.to_lowercase() == "pfl").then(pl.col("qty_1") * 0.789)
                                        .when(pl.col("unit_1").str.to_lowercase() == "gm").then(pl.col("qty_1") * 1000).otherwise(None),

                            conv_2=pl.when(pl.col("unit_2").str.to_lowercase() == "kg").then(pl.col("qty_2") * 1)
                                        .when(pl.col("unit_2").str.to_lowercase() == "l").then(pl.col("qty_2") * 1)
                                        .when(pl.col("unit_2").str.to_lowercase() == "doz").then(pl.col("qty_2") / 0.756)
                                        .when(pl.col("unit_2").str.to_lowercase() == "m3").then(pl.col("qty_2") * 1560)
                                        .when(pl.col("unit_2").str.to_lowercase() == "t").then(pl.col("qty_2") * 907.185)
                                        .when(pl.col("unit_2").str.to_lowercase() == "kts").then(pl.col("qty_2") * 1)
                                        .when(pl.col("unit_2").str.to_lowercase() == "pfl").then(pl.col("qty_2") * 0.789)
                                        .when(pl.col("unit_2").str.to_lowercase() == "gm").then(pl.col("qty_2") * 1000)
                                        .otherwise(None).alias("converted_qty_2"),

                            qrt=pl.when((pl.col("Month") >= 1) & (pl.col("Month") <= 3)).then(1)
                                        .when((pl.col("Month") >= 4) & (pl.col("Month") <= 8)).then(2)
                                        .when((pl.col("Month") >= 7) & (pl.col("Month") <= 9)).then(3)
                                        .when((pl.col("Month") >= 10) & (pl.col("Month") <= 12)).then(4),

                            fiscal_year=pl.when(pl.col("Month") > 6).then(pl.col("Year") + 1)
                                          .otherwise(pl.col("Year")).alias("fiscal_year"))

        df = df.rename({"Year": "year", "Month": "month", "Country": "country", "Commodity_Code": "hs"})
        df = df.with_columns(hs=pl.col("hs").cast(pl.String).str.zfill(10))
        df = df.filter(pl.col("naics") != "RETURN")
        return df


    def process_data(self, switch:list) -> pl.DataFrame:

        match switch:
            case ["yearly", "total"]:

                if os.path.exists(self.saving_dir + "processed/total_yearly.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_yearly.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["year"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")))
                    df = df.select(pl.col("*").exclude("year_right"))
                    df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year")
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case ["yearly", "naics"]:
                if os.path.exists(self.saving_dir + "processed/total_yearly_naics.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_yearly_naics.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["year", "naics"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics")))
                    df = df.select(pl.col("*").exclude("year_right", "naics_right"))
                    df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "naics")
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case ["yearly", "hs"]:
                if os.path.exists(self.saving_dir + "processed/total_yearly_hs.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_yearly_hs.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["year", "hs"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                                hs=pl.when(pl.col("hs").is_null()).then(pl.col("hs_right")).otherwise(pl.col("hs")))
                    df = df.select(pl.col("*").exclude("year_right", "hs_right"))
                    df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "hs")
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case ["yearly", "country"]:
                if os.path.exists(self.saving_dir + "processed/total_yearly_country.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_yearly_country.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["year", "country"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        country=pl.when(pl.col("country").is_null()).then(pl.col("country_right")).otherwise(pl.col("country")))
                    df = df.select(pl.col("*").exclude("year_right", "country_right"))
                    df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "country")
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case ["fiscal", "total"]:
                if os.path.exists(self.saving_dir + "processed/total_fiscal.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_fiscal.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["fiscal_year"])
                    df = df.with_columns(fiscal_year=pl.when(pl.col("fiscal_year").is_null()).then(pl.col("fiscal_year_right")).otherwise(pl.col("fiscal_year")))
                    df = df.select(pl.col("*").exclude("fiscal_year_right"))
                    df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("fiscal_year")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case ["fiscal", "naics"]:
                if os.path.exists(self.saving_dir + "processed/total_fiscal_naics.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_fiscal_naics.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["fiscal_year", "naics"])
                    df = df.with_columns(fiscal_year=pl.when(pl.col("fiscal_year").is_null()).then(pl.col("fiscal_year_right")).otherwise(pl.col("fiscal_year")),
                                        naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics")))
                    df = df.select(pl.col("*").exclude("fiscal_year_right", "naics_right"))
                    df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("fiscal_year", "naics")
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case ["fiscal", "hs"]:
                if os.path.exists(self.saving_dir + "processed/total_fiscal_hs.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_fiscal_hs.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["fiscal_year", "hs"])
                    df = df.with_columns(fiscal_year=pl.when(pl.col("fiscal_year").is_null()).then(pl.col("fiscal_year_right")).otherwise(pl.col("fiscal_year")),
                                        hs=pl.when(pl.col("hs").is_null()).then(pl.col("hs_right")).otherwise(pl.col("hs")))
                    df = df.select(pl.col("*").exclude("fiscal_year_right", "hs_right"))
                    df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("fiscal_year", "hs")
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case ["fiscal", "country"]:
                if os.path.exists(self.saving_dir + "processed/total_fiscal_country.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_fiscal_country.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["fiscal_year", "country"])
                    df = df.with_columns(fiscal_year=pl.when(pl.col("fiscal_year").is_null()).then(pl.col("fiscal_year_right")).otherwise(pl.col("fiscal_year")),
                                        country=pl.when(pl.col("country").is_null()).then(pl.col("country_right")).otherwise(pl.col("country")))
                    df = df.select(pl.col("*").exclude("fiscal_year_right", "country_right"))
                    df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("fiscal_year", "country")
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case ["qrt", "total"]:
                if os.path.exists(self.saving_dir + "processed/total_qrt.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_qrt.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["year", "qrt"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        qrt=pl.when(pl.col("qrt").is_null()).then(pl.col("qrt_right")).otherwise(pl.col("qrt")))
                    df = df.select(pl.col("*").exclude("year_right", "qrt_right"))
                    df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "qrt")
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case ["qrt", "naics"]:
                if os.path.exists(self.saving_dir + "processed/total_qrt_naics.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_qrt_naics.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["year", "qrt", "naics"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        qrt=pl.when(pl.col("qrt").is_null()).then(pl.col("qrt_right")).otherwise(pl.col("qrt")),
                                        naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics")))
                    df = df.select(pl.col("*").exclude("year_right", "qrt_right", "naics_right"))
                    df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "qrt", "naics")
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case ["qrt", "hs"]:
                if os.path.exists(self.saving_dir + "processed/total_qrt_hs.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_qrt_hs.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["year", "qrt", "hs"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        qrt=pl.when(pl.col("qrt").is_null()).then(pl.col("qrt_right")).otherwise(pl.col("qrt")),
                                        hs=pl.when(pl.col("hs").is_null()).then(pl.col("hs_right")).otherwise(pl.col("hs")))
                    df = df.select(pl.col("*").exclude("year_right", "qrt_right", "hs_right"))
                    df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "qrt", "hs")
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case ["qrt", "country"]:
                if os.path.exists(self.saving_dir + "processed/total_qrt_country.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_qrt_country.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["year", "qrt", "country"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        qrt=pl.when(pl.col("qrt").is_null()).then(pl.col("qrt_right")).otherwise(pl.col("qrt")),
                                        country=pl.when(pl.col("country").is_null()).then(pl.col("country_right")).otherwise(pl.col("country")))
                    df = df.select(pl.col("*").exclude("year_right", "qrt_right", "country_right"))
                    df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "qrt", "country")
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case ["monthly", "total"]:
                if os.path.exists(self.saving_dir + "processed/total_monthly.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_monthly.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["year", "month"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        month=pl.when(pl.col("month").is_null()).then(pl.col("month_right")).otherwise(pl.col("month")))
                    df = df.select(pl.col("*").exclude("year_right", "month_right"))
                    df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "month")
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case ["monthly", "naics"]:
                if os.path.exists(self.saving_dir + "processed/total_monthly_naics.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_monthly_naics.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["year", "month", "naics"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        month=pl.when(pl.col("month").is_null()).then(pl.col("month_right")).otherwise(pl.col("month")),
                                        naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics")))
                    df = df.select(pl.col("*").exclude("year_right", "month_right", "naics_right"))
                    return df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "month", "naics")

            case ["monthly", "hs"]:
                if os.path.exists(self.saving_dir + "processed/total_monthly_hs.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_monthly_hs.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["year", "month", "hs"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        month=pl.when(pl.col("month").is_null()).then(pl.col("month_right")).otherwise(pl.col("month")),
                                        hs=pl.when(pl.col("hs").is_null()).then(pl.col("hs_right")).otherwise(pl.col("hs")))
                    df = df.select(pl.col("*").exclude("year_right", "month_right", "hs_right"))
                    return df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "month", "hs")

            case ["monthly", "country"]:
                if os.path.exists(self.saving_dir + "processed/total_monthly_country.parquet"):
                    return pl.read_parquet(self.saving_dir + "processed/total_monthly_country.parquet")
                else:
                    df = self.process_base()
                    df = self.filter_data(df, ["year", "month", "country"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        month=pl.when(pl.col("month").is_null()).then(pl.col("month_right")).otherwise(pl.col("month")),
                                        country=pl.when(pl.col("country").is_null()).then(pl.col("country_right")).otherwise(pl.col("country")))
                    df = df.select(pl.col("*").exclude("year_right", "month_right", "country_right"))
                    df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "month", "country")
                    return df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

            case _:
                raise ValueError(f"Invalid switch: {switch}")

    def process_cat(self, df:pl.DataFrame, switch:list):

        match switch:
            case ["yearly", "total"]:
                df = self.filter_data(df, ["year", "naics"])
                df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                    naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics")))
                df = df.select(pl.col("*").exclude("year_right", "naics_right"))
                df = df.with_columns(pl.col("imports", "exports").fill_null(strategy="zero")).sort("year", "naics")
                df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

                for key, value in self.codes.items():
                    pass

    def filter_data(self, df:pl.DataFrame, filter:list) -> pl.DataFrame:
        imports = df.filter(pl.col("Trade") == "i").group_by(filter).agg(
            pl.sum("data").alias("imports")).sort(filter)
        exports = df.filter(pl.col("Trade") == "e").group_by(filter).agg(
            pl.sum("data").alias("exports")).sort(filter)

        return imports.join(exports, on=filter, how="full", validate="1:1")

