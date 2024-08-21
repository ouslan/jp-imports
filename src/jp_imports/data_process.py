from src.import_export_tools.data_pull import DataPull
import pandas as pd
import numpy as np
import os

class DataProcess(DataPull):

    def __init__(self, saving_dir:str, agriculture:bool, delete_files:bool=True, quarterly:bool=True, totals:bool=False, debug:bool=False) -> None:
        self.saving_dir = saving_dir
        self.agriculture = agriculture
        self.delete_files = delete_files
        self.quarterly = quarterly
        self.totals = totals
        self.debug = debug
        super().__init__(self.saving_dir, self.debug)
        self.process_imp_exp()

    def process_imp_exp(self):
        df_imports = self.process_data(f"{self.saving_dir}raw/import.csv", types="imports")
        df_exports = self.process_data(f"{self.saving_dir}raw/export.csv", types="exports")

        df = pd.merge(df_imports, df_exports, on=['date', 'HTS'], how='outer')

        df = df.drop(columns={"index_x", "index_y"})

        df[["imports", "qty_imports"]] = df[["imports", "qty_imports"]].fillna(0)
        df[["exports", "qty_exports"]] = df[["exports", "qty_exports"]].fillna(0)
        df["net_value"] = df["exports"] - df["imports"]
        df["net_qty"] = df["qty_exports"] - df["qty_imports"]

        df = df.sort_values(by=["HTS", "date"], ascending=True).reset_index(drop=True)

        # Balance the data
        unique_countries = df['HTS'].unique()
        all_dates = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='MS')
        idx = pd.MultiIndex.from_product([unique_countries, all_dates], names=['HTS', 'date'])
        df = df.set_index(['HTS', 'date']).reindex(idx, fill_value=0).reset_index()

        if self.quarterly:
            df = self.to_quarterly(df)

        df.to_parquet("data/processed/imp_exp.parquet")

        if self.delete_files:
            os.remove(f"{self.saving_dir}raw/import.csv")
            os.remove(f"{self.saving_dir}raw/export.csv")

    def process_data(self, data_path:str, types:str) -> pd.DataFrame:
        df = pd.read_csv(data_path, low_memory=False)

        df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str))
        df['value'] = df['value'].astype(float)
        df.drop(['year', 'month'], axis=1, inplace=True)

        # remove illegal characters and remove invalid values
        df['HTS'] = df['HTS'].astype(str)
        df['HTS'] = df['HTS'].str.replace("'", '')
        df['HTS'] = df['HTS'].str.strip()
        df['HTS_dummy'] = df['HTS']
        df['unit_1'] = df['unit_1'].str.lower()
        df['HTS'] = df['HTS'].str[:4]

        # standerize all values to kg 
        df['qty'] = df['qty_1'] + df['qty_2']
        df.drop(['qty_1', 'qty_2'], axis=1, inplace=True)
        df['qty'] = df.apply(self.convertions, axis=1)
        df = df[df['qty'] > 0]

        # group by date and HTS collapse
        df = df.groupby(['date', 'HTS'])[['value', 'qty']].sum().reset_index()
        df = df.sort_values(by=['date','HTS']).reset_index()
        df = df.rename(columns={"value":f"{types}", "qty":f"qty_{types}"})

        # save agriculture product
        if self.agriculture:
            agr = pd.read_json("data/external/agr_hts.json")
            agr = agr.reset_index()
            agr = agr.drop(columns=["index"])
            agr = agr.rename(columns={0:"HTS"})
            agr["HTS"] = agr["HTS"].astype(str).str.zfill(4)
            df = pd.merge(df, agr, on="HTS", how="inner")
            df = df[~df['HTS'].str.startswith(('05', '06', '14'))].reset_index()

        return df

    def to_quarterly(self, df:pd.DataFrame) -> pd.DataFrame:
        df["quarter"] = df["date"].dt.to_period("Q-JUN")
        df_Qyear = df.copy()
        df_Qyear = df_Qyear.drop(['date'], axis=1)
        df_Qyear = df_Qyear.groupby(['HTS', 'quarter']).sum().reset_index()

        return df_Qyear

    def convertions(self, row) -> float:
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
