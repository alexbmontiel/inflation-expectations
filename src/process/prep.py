import pandas as pd
import os
import datetime

import src.config as cfg
from src.acquisition import fred_data


def main():
    fred_data.acquire_fred_data()
    all_data = load_fred_data()
#    all_data = drop_future_dates(all_data)
#    all_data["NROU"] = all_data["NROU"].ffill()
    all_data.to_csv("raw_data.csv")
    print(all_data)


def load_fred_data():
    """Load raw FRED data and join into one dataframe."""
    raw_dir = cfg.DATA_DIR / "raw"

    all_data = pd.DataFrame()
    for pkl in os.listdir(raw_dir):
        df = pd.read_pickle(raw_dir / pkl)

        if len(all_data) == 0:
            all_data = df
        else:
            all_data = all_data.merge(
                df,
                "outer",
                left_index=True,
                right_index=True
            )

    return all_data


def drop_missing_data(dataframe, col):
    """Drop any dates that are later than today."""
    dropped_df = dataframe[dataframe["EXPINF1YR"].notna()]
 
    return dropped_df


def calc_inflation(dataframe, cpi_col):
    """Calculate inflation as a % change (x100) from
    CPI level last month."""
    dataframe = dataframe.assign(
        inflation=lambda x: ((x[cpi_col] - x[cpi_col].shift()) / x[cpi_col].shift())
    )

    return dataframe


def calc_unemployment(dataframe, unemp_col, nrunemp_col):
    """Calculate avg employment vs natural rate of unemployment
    over the past year."""
    dataframe = dataframe.assign(
        nrate_ffilled=lambda x: x[nrunemp_col].ffill(),
        unemp_minus_nrate=lambda x: x[unemp_col] - x.nrate_ffilled,
        unemp_chg=lambda x: x.unemp_minus_nrate.diff()
    )

    return dataframe

def calc_expected_inflation(dataframe, cpi_col, ecpi_col):
    """Calculate expected inflation rate for the next month."""
    dataframe = dataframe.assign(
        ecpi_t12=lambda x: (x[ecpi_col]/100) * x[cpi_col] + x[cpi_col],
        expected_inflation=lambda x: ((x.ecpi_t12 / x[cpi_col])**(1/12) - 1).shift(),
    )

    return dataframe


if __name__ == "__main__":
    main()

