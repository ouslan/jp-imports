import pandas as pd
import polars as pl
import numpy as np
from scipy.stats import zscore
import os


class DataCleaner:

    def country_trade(self, data_path, saving_path, debug=False):
        df = pd.read_csv(data_path, low_memory=False)
        df["date"] = pd.to_datetime(df["Year"].astype(str) + "-" + df["Month"].astype(str))

        df_imports = df[df["Trade"] == "i"].copy().reset_index(drop=True)
        df_imports = df_imports[["Country", "date", "data"]]
        df_imports = df_imports.groupby(['Country', 'date']).sum().reset_index()
        df_imports.rename(columns={"data": "imports"}, inplace=True)

        df_exports = df[df["Trade"] == "e"].copy().reset_index(drop=True)
        df_exports = df_exports[["Country", "date", "data"]]
        df_exports = df_exports.groupby(['Country', 'date']).sum().reset_index()
        df_exports.rename(columns={"data": "exports"}, inplace=True)

        # merge dataframes
        country_trade = pd.merge(df_exports, df_imports, on=["date", "Country"], how="outer")

        # make mising to 0 & make net exports
        country_trade["imports"] = country_trade["imports"].fillna(0)
        country_trade["exports"] = country_trade["exports"].fillna(0)
        country_trade["net_value"] = country_trade["exports"] - country_trade["imports"]

        country_trade = country_trade.sort_values(by=["Country", "date"], ascending=True).reset_index(drop=True)

        # Balance the data
        unique_countries = country_trade['Country'].unique()
        all_dates = pd.date_range(start=country_trade['date'].min(), end=country_trade['date'].max(), freq='MS')
        idx = pd.MultiIndex.from_product([unique_countries, all_dates], names=['Country', 'date'])
        country_trade = country_trade.set_index(['Country', 'date']).reindex(idx, fill_value=0).reset_index()
        
        # save the panel data
        country_trade.to_csv(saving_path)

    def hts_trade(self, imports, exports, saving_path, debug=False):
        df_imports = imports.copy()
        df_exports = exports.copy()
        df_imports.rename(columns={"data": "imports"}, inplace=True)
        df_exports.rename(columns={"data": "exports"}, inplace=True)

        # merge dataframes
        hts_trade = pd.merge(df_exports, df_imports, on=["date", "country"], how="outer")

        # make mising to 0 & make net exports
        hts_trade["imports"] = hts_trade["imports"].fillna(0)
        hts_trade["exports"] = hts_trade["exports"].fillna(0)
        hts_trade["net_value"] = hts_trade["exports"] - hts_trade["imports"]

        hts_trade = hts_trade.sort_values(by=["HTS", "date"], ascending=True).reset_index(drop=True)

        # Balance the data
        unique_countries = net_def['country'].unique()
        all_dates = pd.date_range(start=net_def['date'].min(), end=net_def['date'].max(), freq='MS')
        idx = pd.MultiIndex.from_product([unique_countries, all_dates], names=['Country', 'date'])
        net_def = net_def.set_index(['Country', 'date']).reindex(idx, fill_value=0).reset_index()
        
        # save the panel data
        net_def.to_pickle(saving_path)
        return net_def

    def to_trimester(self, df_path, saving_path):
        df = pd.read_pickle(df_path)
        df["quarter"] = df["date"].dt.to_period("Q-JUN")
        df["Fiscal Year"] = df["quarter"].dt.qyear
        df_Qyear = df.copy()
        df_Qyear = df_Qyear.drop(['date', 'quarter'], axis=1)
        df_Qyear = df_Qyear.groupby(['Country', 'Fiscal Year']).sum().reset_index()

        # save the panel data
        df_Qyear.to_pickle(saving_path)
