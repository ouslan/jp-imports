from .data_pull import DataPull
import polars as pl
import ibis
import os


class DataTrade(DataPull):
    """
    Data processing class for the various data sources in DataPull.
    """

    def __init__(self, database_url:str='sqlite:///db.sqlite', saving_dir:str="data/", dev:bool=False, debug:bool=False):
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
        self.dev = dev
        self.jp_data = os.path.join(self.saving_dir, "raw/jp_data.csv") 
        self.org_data = os.path.join(self.saving_dir, "raw/org_data.parquet")
        self.agr_file = os.path.join(self.saving_dir, "external/agr_data.json")
        super().__init__(database_url=database_url, saving_dir=self.saving_dir, debug=self.debug, dev=self.dev)

    def process_int_jp(self,
                       time:str,
                       types:str,
                       agr:bool=False,
                       group:bool=False,
                       update:bool=False) -> ibis.expr.types.relations.Table:
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

        if "jptradedata" not in self.conn.list_tables() or update:
            self.insert_int_jp(self.jp_data, self.agr_file)
        if int(self.conn.table("jptradedata").count().execute()) == 0:
            self.insert_int_jp(self.jp_data, self.agr_file)

        df = self.conn.table("jptradedata")
        units = self.conn.table("unittable")
        df = self.conversion(df, units)

        if agr:
            hts = self.conn.table("htstable").select("id", "agri_prod")
            df = df.join(hts, df.hts_id == hts.id).filter(pl.col("agri_prod"))

        if group:
            #return self.process_cat(switch=switch)
            raise NotImplementedError("Grouping not implemented yet")
        else:
            return self.process_data(switch=switch, df=df)

    def process_int_org(self, time:str, types:str, agr:bool=False, group:bool=False, update:bool=False) -> ibis.expr.types.relations.Table:
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

        if types == "naics":
            raise ValueError("NAICS data is not available for Puerto Rico Statistics Institute.")
        if "inttradedata" not in self.conn.list_tables() or update:
            self.insert_int_org(self.org_data)
        if int(self.conn.table("inttradedata").count().execute()) == 0:
            self.insert_int_org(self.org_data)

        df = self.conn.table("inttradedata")
        units = self.conn.table("unittable")
        df = self.conversion(df, units)

        if agr:
            hts = self.conn.table("htstable").select("id", "agri_prod")
            df = df.join(hts, df.hts_id == hts.id).filter(df.agri_prod)

        if group:
            #return self.process_cat(switch=switch)
            raise NotImplementedError("Grouping not implemented yet")
        else:
            return self.process_data(switch=switch, df=df)

    def process_data(self, switch:list, df:ibis.expr.types.relations.Table) -> ibis.expr.types.relations.Table:
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
                df = self.filter_data(df, ["year"])
                return df.select(["year", "imports", "exports", "qty_imports", "qty_exports"])

            case ["yearly", "naics"]:
                df = self.filter_data(df, ["year", "naics_id"])
                naics = self.conn.table("naicstable")
                df = df.join(naics, df.naics_id == naics.id)
                return df.select(["year", "naics_id", "naics_code", "imports", "exports", "qty_imports", "qty_exports"])

            case ["yearly", "hts"]:
                df = self.filter_data(df, ["year", "hts_id"])
                hts = self.conn.table("htstable")
                df = df.join(hts, df.hts_id == hts.id)
                return df.select(["year", "hts_id", "hts_code", "imports", "exports", "qty_imports", "qty_exports"])

            case ["yearly", "country"]:
                df = self.filter_data(df, ["year", "country_id"])
                countries = self.conn.table("countrytable")
                df = df.join(countries, df.country_id == countries.id)
                return df.select(["year", "country_id", "cty_code", "country_name", "imports", "exports", "qty_imports", "qty_exports"])

            case ["fiscal", "total"]:
                df = self.filter_data(df, ["fiscal_year"])
                return df.select(["fiscal_year", "imports", "exports", "qty_imports", "qty_exports"])

            case ["fiscal", "naics"]:
                df = self.filter_data(df, ["fiscal_year", "naics_id"])
                naics = self.conn.table("naicstable")
                df = df.join(naics, df.naics_id == naics.id)
                return df.select(["fiscal_year", "naics_id", "naics_code", "imports", "exports", "qty_imports", "qty_exports"])

            case ["fiscal", "hts"]:
                df = self.filter_data(df, ["fiscal_year", "hts_id"])
                hts = self.conn.table("htstable")
                df = df.join(hts, df.hts_id == hts.id)
                return df.select(["fiscal_year", "hts_id", "hts_code", "imports", "exports", "qty_imports", "qty_exports"])

            case ["fiscal", "country"]:
                df = self.filter_data(df, ["fiscal_year", "country_id"])
                countries = self.conn.table("countrytable")
                df = df.join(countries, df.country_id == countries.id)
                return df.select(["fiscal_year", "country_id", "cty_code", "country_name", "imports", "exports", "qty_imports", "qty_exports"])

            case ["qrt", "total"]:
                df = self.filter_data(df, ["year", "qrt"])
                return df.select(["year", "qrt", "imports", "exports", "qty_imports", "qty_exports"])

            case ["qrt", "naics"]:
                df = self.filter_data(df, ["year", "qrt", "naics_id"])
                naics = self.conn.table("naicstable")
                df = df.join(naics, df.naics_id == naics.id)
                return df.select(["year", "qrt", "naics_id", "naics_code", "imports", "exports", "qty_imports", "qty_exports"])

            case ["qrt", "hts"]:
                df = self.filter_data(df, ["year", "qrt", "hts_id"])
                hts = self.conn.table("htstable")
                df = df.join(hts, df.hts_id == hts.id)
                return df.select(["year", "qrt", "hts_id", "hts_code", "imports", "exports", "qty_imports", "qty_exports"])

            case ["qrt", "country"]:
                df = self.filter_data(df, ["year", "qrt", "country_id"])
                countries = self.conn.table("countrytable")
                df = df.join(countries, df.country_id == countries.id)
                return df.select(["year", "qrt", "country_id", "cty_code", "country_name", "imports", "exports", "qty_imports", "qty_exports"])

            case ["monthly", "total"]:
                df = self.filter_data(df, ["year", "month"])
                return df.select(["year", "month", "imports", "exports", "qty_imports", "qty_exports"])

            case ["monthly", "naics"]:
                df = self.filter_data(df, ["year", "month", "naics_id"])
                naics = self.conn.table("naicstable")
                df = df.join(naics, df.naics_id == naics.id)
                return df.select(["year", "month", "naics_id", "naics_code", "imports", "exports", "qty_imports", "qty_exports"])

            case ["monthly", "hts"]:
                df = self.filter_data(df, ["year", "month", "hts_id"])
                hts = self.conn.table("htstable")
                df = df.join(hts, df.hts_id == hts.id)
                return df.select(["year", "month", "hts_id", "hts_code", "imports", "exports", "qty_imports", "qty_exports"])

            case ["monthly", "country"]:
                df = self.filter_data(df, ["year", "month", "country_id"])
                countries = self.conn.table("countrytable")
                df = df.join(countries, df.country_id == countries.id)
                return df.select(["year", "month", "country_id", "cty_code", "country_name", "imports", "exports", "qty_imports", "qty_exports"])

            case _:
                raise ValueError(f"Invalid switch: {switch}")

    def process_price(self, agr:bool=False) -> pl.DataFrame:
        df = self.process_int_org("monthly", "hts", agr).to_polars()
        hts = self.conn.table("htstable").to_polars()
        df = df.join(hts, left_on="hts_id", right_on="id")
        df = df.with_columns(pl.col("qty_imports", "qty_exports").replace(0, 1))
        df = df.with_columns(hs4=pl.col("hts_code").str.slice(0, 4))

        df = df.group_by(pl.col("hs4", "month", "year")).agg(
            pl.col("imports").sum().alias("imports"),
            pl.col("exports").sum().alias("exports"), 
            pl.col("qty_imports").sum().alias("qty_imports"), 
            pl.col("qty_exports").sum().alias("qty_exports")
        )

        df = df.with_columns(
            price_imports=pl.col("imports") / pl.col("qty_imports"),
            price_exports=pl.col("exports") / pl.col("qty_exports")
        )

        df = df.with_columns(date=pl.datetime(pl.col("year"), pl.col("month"), 1))

        # Sort the DataFrame by the date column
        df = df.sort("date")

        # Now you can safely use group_by_dynamic
        result = df.with_columns(
            pl.col("price_imports").rolling_mean(window_size=3, min_periods=1).over("hs4").alias("moving_price_imports"),
            pl.col("price_exports").rolling_mean(window_size=3, min_periods=1).over("hs4").alias("moving_price_exports"), 
            pl.col("price_imports").rolling_std(window_size=3, min_periods=1).over("hs4").alias("moving_price_imports_std"),
            pl.col("price_exports").rolling_std(window_size=3, min_periods=1).over("hs4").alias("moving_price_exports_std"), 
        )
        results = result.with_columns(
            pl.col("moving_price_imports").rank("ordinal").over("date").alias("rank_imports").cast(pl.Int64), 
            pl.col("moving_price_exports").rank("ordinal").over("date").alias("rank_exports").cast(pl.Int64),
            upper_band_imports = pl.col("moving_price_imports") + 2 * pl.col("moving_price_imports_std"),
            lower_band_imports = pl.col("moving_price_imports") - 2 * pl.col("moving_price_imports_std"),
            upper_band_exports = pl.col("moving_price_exports") + 2 * pl.col("moving_price_exports_std"),
            lower_band_exports = pl.col("moving_price_exports") - 2 * pl.col("moving_price_exports_std"),
            )
        results = df.join(results, on=["date", "hs4"], how="left", validate="1:1")

        # Assuming 'results' already has the necessary columns and is sorted by date and hs4
        tmp = results.with_columns(
            pl.col("moving_price_imports").pct_change().over("date", "hs4").alias("pct_change_imports")
        ).sort(by=["date", "hs4"])

        # To get the percentage change for the same month of the previous year
        # First, create a column for the previous year's value
        tmp = tmp.with_columns(
            pl.when(pl.col("date").dt.year() > 1)  # Ensure there's a previous year to compare
            .then(pl.col("moving_price_imports").shift(12))  # Shift by 12 months
            .otherwise(None)
            .alias("prev_year_imports"), 
        )
        tmp = tmp.with_columns(
            pl.when(pl.col("date").dt.year() > 1)  # Ensure there's a previous year to compare
            .then(pl.col("moving_price_exports").shift(12))  # Shift by 12 months
            .otherwise(None)
            .alias("prev_year_exports"), 
        )
        tmp = tmp.with_columns(
            pl.when(pl.col("date").dt.year() > 1)  # Ensure there's a previous year to compare
            .then(pl.col("rank_imports").shift(12))  # Shift by 12 months
            .otherwise(None)
            .alias("prev_year_rank_imports"), 
        )
        tmp = tmp.with_columns(
            pl.when(pl.col("date").dt.year() > 1)  # Ensure there's a previous year to compare
            .then(pl.col("rank_exports").shift(12))  # Shift by 12 months
            .otherwise(None)
            .alias("prev_year_rank_exports") 
        )

        # Now calculate the percentage change
        tmp = tmp.with_columns(
            ((pl.col("moving_price_imports") - pl.col("prev_year_imports")) / pl.col("prev_year_imports")).alias("pct_change_imports_year_over_year"),
            ((pl.col("moving_price_exports") - pl.col("prev_year_exports")) / pl.col("prev_year_exports")).alias("pct_change_exports_year_over_year"),
            (pl.col("rank_imports") - pl.col("prev_year_rank_imports")).alias("rank_imports_change_year_over_year"),
            (pl.col("rank_exports").cast(pl.Int64) - pl.col("prev_year_rank_exports").cast(pl.Int64)).alias("rank_exports_change_year_over_year")
        ).sort(by=["date", "hs4"])
        return tmp

    def process_cat(self, df:pl.DataFrame, switch:list):

        match switch:
            case ["yearly", "total"]:
                df = self.filter_data(df, ["year", "naics"])
                df = df.with_columns(year=pl.when(pl.col("year").is_null()).then(pl.col("year_right")).otherwise(pl.col("year")),
                                    naics=pl.when(pl.col("naics").is_null()).then(pl.col("naics_right")).otherwise(pl.col("naics")))
                df = df.select(pl.col("*").exclude("year_right", "naics_right"))
                df = df.with_columns(pl.col("imports", "exports", "qty_imports", "qty_exports").fill_null(strategy="zero")).sort("year", "naics")
                df = df.with_columns(net_exports=pl.col("exports")-pl.col("imports"))

    def filter_data(self, df:ibis.expr.types.relations.Table, filters:list) -> ibis.expr.types.relations.Table:
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

        imports = df.filter(df.trade_id == 1).group_by(filters).aggregate(
            [df.data.sum().name("imports"), df.qty.sum().name("qty_imports")]
        )
        exports = df.filter(df.trade_id == 2).group_by(filters).aggregate(
            [df.data.sum().name("exports"), df.qty.sum().name("qty_exports")]
        )
        df = imports.join(exports, filters)
        df = df.mutate(
            net_exports=df.exports - df.imports,
            net_qty=df.qty_exports - df.qty_imports,
        )

        return df

    def conversion(self, df:ibis.expr.types.relations.Table, units:ibis.expr.types.relations.Table) -> ibis.expr.types.relations.Table:
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

        df = df.join(units, df.unit1_id == units.id).rename(unit_1="unit_code")
        df = df.join(units, df.unit2_id == units.id).rename(unit_2="unit_code")
        df = df.mutate(
            conv_1=ibis.case()
                .when(df.unit_1 == "kg", df.qty_1 * 1)
                .when(df.unit_1 == "l", df.qty_1 * 1)
                .when(df.unit_1 == "doz", df.qty_1 / 0.756)
                .when(df.unit_1 == "m3", df.qty_1 * 1560)
                .when(df.unit_1 == "t", df.qty_1 * 907.185)
                .when(df.unit_1 == "kts", df.qty_1 * 1)
                .when(df.unit_1 == "pfl", df.qty_1 * 0.789)
                .when(df.unit_1 == "gm", df.qty_1 * 1000)
                .else_(df.qty_1)
                .end(),
            conv_2=ibis.case()
                .when(df.unit_2 == "kg", df.qty_2 * 1)
                .when(df.unit_2 == "l", df.qty_2 * 1)
                .when(df.unit_2 == "doz", df.qty_2 / 0.756)
                .when(df.unit_2 == "m3", df.qty_2 * 1560)
                .when(df.unit_2 == "t", df.qty_2 * 907.185)
                .when(df.unit_2 == "kts", df.qty_2 * 1)
                .when(df.unit_2 == "pfl", df.qty_2 * 0.789)
                .when(df.unit_2 == "gm", df.qty_2 * 1000)
                .else_(df.qty_2)
                .end(),
            qrt=ibis.case()
                .when((df.date.month() >= 1) & (df.date.month() <= 3), 1)
                .when((df.date.month() >= 4) & (df.date.month() <= 8), 2)
                .when((df.date.month() >= 7) & (df.date.month() <= 9), 3)
                .when((df.date.month() >= 10) & (df.date.month() <= 12), 4)
                .else_(0)
                .end(),
            fiscal_year=ibis.case()
                .when(df.date.month() > 6, df.date.year() + 1)
                .else_(df.date.year())
                .end())
        df = df.mutate(
            qty=df.conv_1 + df.conv_2,
            year=df.date.year(),
            month=df.date.month(),
            )

        return df
