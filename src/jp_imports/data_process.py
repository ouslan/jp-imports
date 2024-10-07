from .data_pull import DataPull
import polars as pl
import json
import os

class DataProcess(DataPull):
    """
    Data processing class for the various data sources in DataPull.
    """

    def __init__(self, saving_dir:str, debug:bool=False):
        """
        Initialize the DataProcess class.

        Parameters
        ----------
        saving_dir: str
            Directory to save the data.
        debug: bool
            Will print debug information in the console if True.

        Returns
        -------
        None
        """
        self.saving_dir = saving_dir
        self.debug = debug
        super().__init__(saving_dir=self.saving_dir)
        self.codes = json.load(open(self.saving_dir + "external/code_classification.json"))
        self.codes_agr = []

        agr = json.load(open(self.saving_dir + "external/code_agr.json"))

        for i in list(agr.values()):
            self.codes_agr.append(str(i).zfill(4))

    def process_int_jp(self, time:str, types:str, agr:bool=False, group:bool=False, update:bool=False) -> pl.LazyFrame:
        """
        Process the data for Puerto Rico Statistics Institute provided to JP.

        Parameters
        ----------
        time: str
            Time period to process the data. The options are "yearly", "qrt", and "monthly".
        types: str
            Type of data to process. The options are "total", "naics", "hs", and "country".
        group: bool
            Group the data by the classification. (Not implemented yet)
        update: bool
            Update the data from the source.

        Returns
        -------
        pl.LazyFrame
            Processed data. Requires df.collect() to view the data.
        """
        switch = [time, types]
        if not os.path.exists(self.saving_dir + "raw/jp_instance.parquet") or update:
            self.pull_int_jp()

        if group:
            #return self.process_cat(switch=switch)
            raise NotImplementedError("Grouping not implemented yet")
        else:
            return self.process_data(switch=switch, base=self.process_jp_base(agr=agr))

    def process_int_org(self, time:str, types:str, agr:bool=False, group:bool=False, update:bool=False) -> pl.LazyFrame:
        """
        Process the data from Puerto Rico Statistics Institute.

        Parameters
        ----------
        time: str
            Time period to process the data. The options are "yearly", "qrt", and "monthly".
        types: str
            Type of data to process. The options are "total", "naics", "hs", and "country".
        group: bool
            Group the data by the classification. (Not implemented yet)
        update: bool
            Update the data from the source.

        Returns
        -------
        pl.LazyFrame
            Processed data. Requires df.collect() to view the data.
        """
        switch = [time, types]
        if not os.path.exists(self.saving_dir + "raw/int_instance.parquet") or update:
            self.pull_int_org()

        if group:
            #return self.process_cat(switch=switch)
            raise NotImplementedError("Grouping not implemented yet")
        else:
            return self.process_data(switch=switch, base=self.process_int_base(agr=agr))

    def process_data(self, switch:list, base:pl.lazyframe) -> pl.LazyFrame:
        """
        Process the data based on the switch. Used for the process_int_jp and process_int_org methods 
            to determine the aggregation of the data.

        Parameters
        ----------
        switch: list
            List of strings to determine the aggregation of the data based on the time and type from 
            the process_int_jp and process_int_org methods.
        base: pl.lazyframe
            The pre-procesed and staderized data to process. This data comes from the process_int_jp and process_int_org methods.

        Returns
        -------
        pl.LazyFrame
            Processed data. Requires df.collect() to view the data.
        """

        match switch:
            case ["yearly", "total"]:
                    df = self.filter_data(base, ["year"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")))
                    df = df.select(pl.col("*").exclude("year_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("year")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["yearly", "naics"]:
                    df = self.filter_data(base, ["year", "naics"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics")))
                    df = df.select(pl.col("*").exclude("year_right", "naics_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("year", "naics")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["yearly", "hs"]:
                    df = self.filter_data(base, ["year", "hs"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                                hs=pl.when(pl.col("hs").is_null()).then(pl.col("hs_right")).otherwise(pl.col("hs")))
                    df = df.select(pl.col("*").exclude("year_right", "hs_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("year", "hs")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["yearly", "country"]:
                    df = self.filter_data(base, ["year", "country"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        country=pl.when(pl.col("country").is_null()).then(pl.col("country_right")).otherwise(pl.col("country")))
                    df = df.select(pl.col("*").exclude("year_right", "country_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("year", "country")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["fiscal", "total"]:
                    df = self.filter_data(base, ["fiscal_year"])
                    df = df.with_columns(fiscal_year=pl.when(pl.col("fiscal_year").is_null()).then(pl.col("fiscal_year_right")).otherwise(pl.col("fiscal_year")))
                    df = df.select(pl.col("*").exclude("fiscal_year_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("fiscal_year")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["fiscal", "naics"]:
                    df = self.filter_data(base, ["fiscal_year", "naics"])
                    df = df.with_columns(fiscal_year=pl.when(pl.col("fiscal_year").is_null()).then(pl.col("fiscal_year_right")).otherwise(pl.col("fiscal_year")),
                                        naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics")))
                    df = df.select(pl.col("*").exclude("fiscal_year_right", "naics_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("fiscal_year", "naics")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["fiscal", "hs"]:
                    df = self.filter_data(base, ["fiscal_year", "hs"])
                    df = df.with_columns(fiscal_year=pl.when(pl.col("fiscal_year").is_null()).then(pl.col("fiscal_year_right")).otherwise(pl.col("fiscal_year")),
                                        hs=pl.when(pl.col("hs").is_null()).then(pl.col("hs_right")).otherwise(pl.col("hs")))
                    df = df.select(pl.col("*").exclude("fiscal_year_right", "hs_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("fiscal_year", "hs")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["fiscal", "country"]:
                    df = self.filter_data(base, ["fiscal_year", "country"])
                    df = df.with_columns(fiscal_year=pl.when(pl.col("fiscal_year").is_null()).then(pl.col("fiscal_year_right")).otherwise(pl.col("fiscal_year")),
                                        country=pl.when(pl.col("country").is_null()).then(pl.col("country_right")).otherwise(pl.col("country")))
                    df = df.select(pl.col("*").exclude("fiscal_year_right", "country_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("fiscal_year", "country")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["qrt", "total"]:
                    df = self.filter_data(base, ["year", "qrt"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        qrt=pl.when(pl.col("qrt").is_null()).then(pl.col("qrt_right")).otherwise(pl.col("qrt")))
                    df = df.select(pl.col("*").exclude("year_right", "qrt_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("year", "qrt")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["qrt", "naics"]:
                    df = self.filter_data(base, ["year", "qrt", "naics"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        qrt=pl.when(pl.col("qrt").is_null()).then(pl.col("qrt_right")).otherwise(pl.col("qrt")),
                                        naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics")))
                    df = df.select(pl.col("*").exclude("year_right", "qrt_right", "naics_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("year", "qrt", "naics")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["qrt", "hs"]:
                    df = self.filter_data(base, ["year", "qrt", "hs"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        qrt=pl.when(pl.col("qrt").is_null()).then(pl.col("qrt_right")).otherwise(pl.col("qrt")),
                                        hs=pl.when(pl.col("hs").is_null()).then(pl.col("hs_right")).otherwise(pl.col("hs")))
                    df = df.select(pl.col("*").exclude("year_right", "qrt_right", "hs_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("year", "qrt", "hs")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["qrt", "country"]:
                    df = self.filter_data(base, ["year", "qrt", "country"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        qrt=pl.when(pl.col("qrt").is_null()).then(pl.col("qrt_right")).otherwise(pl.col("qrt")),
                                        country=pl.when(pl.col("country").is_null()).then(pl.col("country_right")).otherwise(pl.col("country")))
                    df = df.select(pl.col("*").exclude("year_right", "qrt_right", "country_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("year", "qrt", "country")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["monthly", "total"]:
                    df = self.filter_data(base, ["year", "month"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        month=pl.when(pl.col("month").is_null()).then(pl.col("month_right")).otherwise(pl.col("month")))
                    df = df.select(pl.col("*").exclude("year_right", "month_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("year", "month")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["monthly", "naics"]:
                    df = self.filter_data(base, ["year", "month", "naics"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        month=pl.when(pl.col("month").is_null()).then(pl.col("month_right")).otherwise(pl.col("month")),
                                        naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics")))
                    df = df.select(pl.col("*").exclude("year_right", "month_right", "naics_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("year", "month", "naics")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["monthly", "hs"]:
                    df = self.filter_data(base, ["year", "month", "hs"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        month=pl.when(pl.col("month").is_null()).then(pl.col("month_right")).otherwise(pl.col("month")),
                                        hs=pl.when(pl.col("hs").is_null()).then(pl.col("hs_right")).otherwise(pl.col("hs")))
                    df = df.select(pl.col("*").exclude("year_right", "month_right", "hs_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("year", "month", "hs")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case ["monthly", "country"]:
                    df = self.filter_data(base, ["year", "month", "country"])
                    df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                        month=pl.when(pl.col("month").is_null()).then(pl.col("month_right")).otherwise(pl.col("month")),
                                        country=pl.when(pl.col("country").is_null()).then(pl.col("country_right")).otherwise(pl.col("country")))
                    df = df.select(pl.col("*").exclude("year_right", "month_right", "country_right"))
                    df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("year", "month", "country")
                    df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))
                    df = df.with_columns(net_qty=pl.col("exports_qty")-pl.col("imports_qty"))
                    return df

            case _:
                raise ValueError(f"Invalid switch: {switch}")

    def process_jp_base(self, agr:bool=False) -> pl.LazyFrame:
        """
        Process the data for Puerto Rico Statistics Institute provided to JP. Standardize the data and filter out 
            the data that is not needed.

        Parameters
        ----------
        agr: bool
            Filter the data for agriculture only.

        Returns
        -------
        pl.lazyframe
            Processed data. Requires df.collect() to view the data.
        """
        df = pl.scan_parquet(self.saving_dir + "raw/jp_instance.parquet")

        df = df.rename({"Year": "year", "Month": "month", "Country": "country", "Commodity_Code": "hs"})
        df = self.conversion(df)

        df = df.with_columns(hs=pl.col("hs").cast(pl.String).str.replace("'", "").str.zfill(10))
        df = df.filter(pl.col("naics") != "RETURN")

        if agr:
            return df.filter(pl.col("hs").str.slice(0, 4).is_in(self.codes_agr))
        else:
            return df

    def process_int_base(self, agr:bool=False) -> pl.LazyFrame:
        """
        Process the data from Puerto Rico Statistics Institute. Standardize the data and filter out
            the data that is not needed.

        Parameters
        ----------
        agr: bool
            Filter the data for agriculture only.

        Returns
        -------
        pl.lazyframe
            Processed data. Requires df.collect() to view the data.
        """
        df = pl.scan_parquet(self.saving_dir + "raw/int_instance.parquet")

        df = df.rename({"import_export": "Trade", "value": "data", "HTS": "hs"})
        df = self.conversion(df)

        df = df.with_columns(hs=pl.col("hs").cast(pl.String).str.replace("'", "").str.zfill(10))

        if agr:
            return df.filter(pl.col("hs").str.slice(0, 4).is_in(self.codes_agr))
        else:
            return df

    def process_cat(self, df:pl.DataFrame, switch:list):

        match switch:
            case ["yearly", "total"]:
                df = self.filter_data(df, ["year", "naics"])
                df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                    naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics")))
                df = df.select(pl.col("*").exclude("year_right", "naics_right"))
                df = df.with_columns(pl.col("imports", "exports", "imports_qty", "exports_qty").fill_null(strategy="zero")).sort("year", "naics")
                df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

                for key, value in self.codes.items():
                    pass

    def filter_data(self, df:pl.DataFrame, filter:list) -> pl.DataFrame:
        """
        Filter the data based on the filter list.

        Parameters
        ----------
        df: pl.DataFrame
            Data to filter.
        filter: List
            List of columns to filter the data.

        Returns
        -------
        pl.DataFrame
            data to be filtered.
        """
        imports = df.filter(pl.col("Trade") == "i").group_by(filter).agg(
            pl.sum("data", "qty")).sort(filter).rename({"data": "imports", "qty": "imports_qty"})
        exports = df.filter(pl.col("Trade") == "e").group_by(filter).agg(
            pl.sum("data", "qty")).sort(filter).rename({"data": "exports", "qty": "exports_qty"})

        return imports.join(exports, on=filter, how="full", validate="1:1")

    def conversion(self, df:pl.LazyFrame) -> pl.LazyFrame:
        """
        Convert the data to the correct units (kg).

        Parameters
        ----------
        df: pl.LazyFrame
            Data to convert.

        Returns
        -------
        pl.LazyFrame
            Converted data.
        """

        df = df.with_columns(pl.col("qty_1", "qty_2").fill_null(strategy="zero"))
        df = df.with_columns(conv_1=pl.when(pl.col("unit_1").str.to_lowercase() == "kg").then(pl.col("qty_1") * 1)
                                        .when(pl.col("unit_1").str.to_lowercase() == "l").then(pl.col("qty_1") * 1)
                                        .when(pl.col("unit_1").str.to_lowercase() == "doz").then(pl.col("qty_1") / 0.756)
                                        .when(pl.col("unit_1").str.to_lowercase() == "m3").then(pl.col("qty_1") * 1560)
                                        .when(pl.col("unit_1").str.to_lowercase() == "t").then(pl.col("qty_1") * 907.185)
                                        .when(pl.col("unit_1").str.to_lowercase() == "kts").then(pl.col("qty_1") * 1)
                                        .when(pl.col("unit_1").str.to_lowercase() == "pfl").then(pl.col("qty_1") * 0.789)
                                        .when(pl.col("unit_1").str.to_lowercase() == "gm").then(pl.col("qty_1") * 1000)
                                        .otherwise(pl.col("qty_1")),

                            conv_2=pl.when(pl.col("unit_2").str.to_lowercase() == "kg").then(pl.col("qty_2") * 1)
                                        .when(pl.col("unit_2").str.to_lowercase() == "l").then(pl.col("qty_2") * 1)
                                        .when(pl.col("unit_2").str.to_lowercase() == "doz").then(pl.col("qty_2") / 0.756)
                                        .when(pl.col("unit_2").str.to_lowercase() == "m3").then(pl.col("qty_2") * 1560)
                                        .when(pl.col("unit_2").str.to_lowercase() == "t").then(pl.col("qty_2") * 907.185)
                                        .when(pl.col("unit_2").str.to_lowercase() == "kts").then(pl.col("qty_2") * 1)
                                        .when(pl.col("unit_2").str.to_lowercase() == "pfl").then(pl.col("qty_2") * 0.789)
                                        .when(pl.col("unit_2").str.to_lowercase() == "gm").then(pl.col("qty_2") * 1000)
                                        .otherwise(pl.col("qty_2")),

                            qrt=pl.when((pl.col("month") >= 1) & (pl.col("month") <= 3)).then(1)
                                        .when((pl.col("month") >= 4) & (pl.col("month") <= 8)).then(2)
                                        .when((pl.col("month") >= 7) & (pl.col("month") <= 9)).then(3)
                                        .when((pl.col("month") >= 10) & (pl.col("month") <= 12)).then(4),

                            fiscal_year=pl.when(pl.col("month") > 6).then(pl.col("year") + 1)
                                          .otherwise(pl.col("year")).alias("fiscal_year")).with_columns(qty=pl.col("conv_1") + pl.col("conv_2"))
        return df
