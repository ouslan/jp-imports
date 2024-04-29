from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import pandas as pd
import itertools

# RAW DATA
df = pd.read_pickle("data/raw/import.pkl")
df["date"] = pd.to_datetime(df["year"].astype(str) + "-" + df["month"].astype(str))
df2 = df[["country", "date", "value"]].copy()
df2["quarter"] = df2["date"].dt.to_period("Q-JUN")
df2["Fiscal Year"] = df2["quarter"].dt.qyear

unique_countries = df2["country"].unique()
#  EDZEL DATA
df_test = pd.read_csv("data/external/us_ExIm.csv")
df_test = df_test[["year", "country", "imports"]]


def filter_transactions(df, value):
    return df[df["value"] <= value]


def check_combination(raw_df, combination, value):
    if raw_df.loc[list(combination), "value"].sum() == value:
        return raw_df.loc[list(combination)]


def find_combination(raw_df, value):
    with ThreadPoolExecutor() as executor:
        for length in range(1, len(raw_df) + 1):
            futures = [
                executor.submit(check_combination, raw_df, combination, value)
                for combination in itertools.combinations(raw_df.index, length)
                if not any(raw_df.loc[i, "value"] > value for i in combination)
                and raw_df.loc[list(combination), "value"].sum() == value
            ]
            print(f"Checking combinations of length {length}.")
            for future in as_completed(futures):
                results = future.result()  # results is now a DataFrame
                if not results.empty:
                    return (
                        results  # Return immediately when a valid combination is found
                    )
    return pd.DataFrame()  # Return an empty DataFrame if no valid combination is found


def adjustment(raw, target, year, country):
    if country not in raw["country"].unique():
        raise ValueError(f"Country {country} does not exist in the dataframe.")

    # get target data
    target = target[(target["country"] == country) & (target["year"] == year)]
    target.columns.values[2] = "value"
    print(target)
    target_value = target.values[0][2]

    # get raw data frame
    raw_df = raw[(raw["country"] == country) & (raw["Fiscal Year"] == year)]
    raw_value = raw_df["value"].sum()

    value = raw_value - target_value
    print(f"Value to adjust: {value}")

    if value <= 0:
        print("No adjustment needed. The value is negative.")
        return pd.DataFrame()  # Return an empty DataFrame if no adjustment is needed
    # Find transactions that sum to the value

    raw_df = filter_transactions(raw_df, target_value)

    combination = find_combination(raw_df, value)

    if combination is None:
        print(f"No combination of transactions found that sum to {value}.")
        return pd.DataFrame()  # Return an empty DataFrame if no combination is found
    else:
        return combination  # Return the DataFrame with the found combination


# new_df = pd.DataFrame()

# for year in range(2010, 2015):
#     for country in unique_countries:
#         new_df = pd.concat([new_df, adjustment(df2, df_test, year, country)])
#         print(f"Finished adjustment for year {year} in country {country}.")

# print(new_df)

if __name__ == "__main__":

    new_df = pd.DataFrame()
    # Usage:

    # Case: Adjustment needed Combination found of 2 transactions
    new_df = pd.concat([new_df, adjustment(df2, df_test, 2014, "Israel")])
    # new_df = pd.concat([new_df, adjustment(df2, df_test, 2014, "Israel")])

    # Case: Adjustment not needed is negative
    # adjustment(df2, df_test, 2014, 'Spain')

    # Case: Adjustment not needed is zero
    # adjustment(df2, df_test, 2014, 'Russia')

    # Case: Don't stop searching for combinations
    # adjustment(df2, df_test, 2016, "United States")
    # adjustment(df2, df_test, 2014, 'Ireland')

    print(new_df)
