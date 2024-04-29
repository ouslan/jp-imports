import pandas as pd
import polars as pl
import numpy as np
from scipy.stats import zscore
import os


class DataCleaner:

    def country_trade(self, data_path, saving_path, debug=False):
        df = pd.read_csv(data_path, low_memory=False)
        df["date"] = pd.to_datetime(df["Year"].astype(str) + "-" + df["Month"].astype(str))
        df["date"] = pd.to_datetime(df["date"])

        df_imports = df[df["Trade"] == "i"].copy().reset_index(drop=True)
        df_imports = df_imports[["Country", "date", "data", 'qty_1', 'qty_2', "unit_1"]]

        # standerize all values to kg 
        df_imports['qty_imp'] = df_imports['qty_1'] + df_imports['qty_2']
        df_imports.drop(['qty_1', 'qty_2', "unit_1"], axis=1, inplace=True)
        df_imports = df_imports[df_imports['qty_imp'] > 0].copy()
        df_imports = df_imports.groupby(['Country', 'date']).sum().reset_index()
        df_imports.rename(columns={"data": "imports"}, inplace=True)

        df_exports = df[df["Trade"] == "e"].copy().reset_index(drop=True)
        df_exports = df_exports[["Country", "date", "data", "qty_1", "qty_2"]]

        # standerize all values to kg 
        df_exports['qty_exp'] = df_exports['qty_1'] + df_exports['qty_2']
        df_exports.drop(['qty_1', 'qty_2'], axis=1, inplace=True)
        df_exports = df_exports[df_exports['qty_exp'] > 0].copy()
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

        # Get growth rate
        country_trade['value_per_unit'] = country_trade['imports'] / country_trade['qty_imp']
        country_trade['value_rolling'] = country_trade['value_per_unit'].rolling(window=3).mean()
        country_trade['value_growth %'] = country_trade.groupby(['Country'])['value_rolling'].pct_change(periods=12, fill_method=None).mul(100)
        
        # save the panel data
        country_trade.to_csv(saving_path)

    def hts_trade(self, data_path, saving_path, debug=False):
        
        df = pd.read_csv(data_path, low_memory=False)
        df["date"] = pd.to_datetime(df["Year"].astype(str) + "-" + df["Month"].astype(str))

        df_imports = df[df["Trade"] == "i"].copy().reset_index(drop=True)
        df_imports = df_imports[["Commodity_Code", "date", "data", 'qty_1', 'qty_2', "unit_1"]]

        # standerize all values to kg 
        df_imports['qty_imp'] = df_imports['qty_1'] + df_imports['qty_2']
        df_imports.drop(['qty_1', 'qty_2', 'unit_1'], axis=1, inplace=True)
        df_imports = df_imports[df_imports['qty_imp'] > 0].copy()
        df_imports = df_imports.groupby(['Commodity_Code', 'date']).sum().reset_index()
        df_imports.rename(columns={"data": "imports"}, inplace=True)

        df_exports = df[df["Trade"] == "e"].copy().reset_index(drop=True)
        df_exports = df_exports[["Commodity_Code", "date", "data", 'qty_1', 'qty_2', "unit_1"]]

        # standerize all values to kg 
        df_exports['qty_exp'] = df_exports['qty_1'] + df_exports['qty_2']
        df_exports.drop(['qty_1', 'qty_2', "unit_1"], axis=1, inplace=True)
        df_exports = df_exports[df_exports['qty_exp'] > 0].copy()
        df_exports = df_exports.groupby(['Commodity_Code', 'date']).sum().reset_index()
        df_exports.rename(columns={"data": "exports"}, inplace=True)

        # merge dataframes
        hts_trade = pd.merge(df_exports, df_imports, on=["date", "Commodity_Code"], how="outer")

        # make mising to 0 & make net exports
        hts_trade["imports"] = hts_trade["imports"].fillna(0)
        hts_trade["exports"] = hts_trade["exports"].fillna(0)
        hts_trade["net_value"] = hts_trade["exports"] - hts_trade["imports"]

        hts_trade = hts_trade.sort_values(by=["Commodity_Code", "date"], ascending=True).reset_index(drop=True)

        # Balance the data
        unique_countries = hts_trade['Commodity_Code'].unique()
        all_dates = pd.date_range(start=hts_trade['date'].min(), end=hts_trade['date'].max(), freq='MS')
        idx = pd.MultiIndex.from_product([unique_countries, all_dates], names=['Commodity_Code', 'date'])
        hts_trade = hts_trade.set_index(['Commodity_Code', 'date']).reindex(idx, fill_value=0).reset_index()
        
        # Get growth rate
        hts_trade['value_per_unit'] = hts_trade['imports'] / hts_trade['qty_imp']
        hts_trade['value_rolling'] = hts_trade['value_per_unit'].rolling(window=3).mean()
        hts_trade['value_growth %'] = hts_trade.groupby(['Commodity_Code'])['value_rolling'].pct_change(periods=12, fill_method=None).mul(100)

        # save the panel data
        hts_trade.to_csv(saving_path)

    def to_trimester(self, df_path, saving_path):
        df = pd.read_pickle(df_path)
        df["quarter"] = df["date"].dt.to_period("Q-JUN")
        df["Fiscal Year"] = df["quarter"].dt.qyear
        df_Qyear = df.copy()
        df_Qyear = df_Qyear.drop(['date', 'quarter'], axis=1)
        df_Qyear = df_Qyear.groupby(['Country', 'Fiscal Year']).sum().reset_index()

        # save the panel data
        df_Qyear.to_pickle(saving_path)

    def convertions(self, row):
        if row['unit_1'] == 'kg':
            return row['qty'] * 1
        elif row['unit_1'] == 'l':
            return row['qty'] * 1
        elif row['unit_1'] == 'doz':
            return row['qty'] / 0.756
        elif row['unit_1'] =='m3':
            return row['qty'] * 1560
        elif row['unit_1'] == 't':
            return row['qty'] * 907.185
        elif row['unit_1'] == 'kts':
            return row['qty'] * 1
        elif row['unit_1'] == 'pfl':
            return row['qty'] * 0.789
        elif row['unit_1'] == 'gm':
            return row['qty'] * 1000
        else:
            return np.nan