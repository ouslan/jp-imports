import pandas as pd
import numpy as np
from scipy.stats import zscore


class DataTrans:
    
    def __init__(self, data_path):
        self.data = pd.read_csv(data_path)

    def clean(self, debug=False):
        df = self.data.copy()
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
        df = df[~df['HTS'].str.startswith(('05', '06', '14'))]
        
        # standerize all values to kg 
        df['qty'] = df['qty_1'] + df['qty_2']
        df.drop(['qty_1', 'qty_2'], axis=1, inplace=True)
        df['qty'] = df.apply(self.convertions, axis=1)
        df = df[df['qty'] > 0]

        # remove outliers
        # codes = df['HTS'].unique()
        # theshold = 2
        # for code in codes:
        #     df_code = df[df['HTS'] == code]
        #     df_code = df_code[['HTS', 'value', 'qty']]

        #     # remove outliers
        #     score_value = zscore(df_code['value'])
        #     score_qty = zscore(df_code['qty'])
        #     df_code_value = score_value[score_value > theshold]
        #     df_code_qty = score_qty[score_qty > theshold]
        #     df_code = pd.concat([df_code_value, df_code_qty], axis=1)
        #     df_code = df_code.drop_duplicates()
        #     df.drop(df_code.index, inplace=True)
        df.reset_index()

        # save checkpoints
        df_iterm = df.copy()

        # group by date and HTS collapse
        df = df.groupby(['date', 'HTS'])[['value', 'qty']].sum().reset_index()
        df = df.sort_values(by=['date','HTS']).reset_index()
        df['value_per_unit'] = df['value'] / df['qty']
        df['value_growth'] = df.groupby(['HTS'])['value'].pct_change()
        df = df[['date', 'HTS', 'qty', 'value', 'value_growth', 'value_per_unit']]
        df_labels = df_iterm[['HTS', 'HTS_desc']].reset_index()
        df_labels = df_labels.drop_duplicates(subset=['HTS']).reset_index()
        df = pd.merge(df, df_labels, on='HTS', how='left')
        
        if debug:
            return df, df_iterm
        else:
            df.to_csv('data/processed/data_trans.csv', index=False)

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

if __name__ == "__main__":
    c = pd.read_csv('Data/imports_data.csv')
    d = DataTrans(c)
    print(d.clean(d.data).head(10))