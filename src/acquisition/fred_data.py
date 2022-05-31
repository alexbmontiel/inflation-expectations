import pandas as pd
import os
import datetime
import pickle
from pathlib import Path
from fredapi import Fred

import src.config as cfg


FRED = Fred(api_key=cfg.FRED_KEY)
DATA_DIR = Path(__file__).parent.parent.resolve() / "data" / "raw"
FRED_DATA = [
    "EXPINF1YR",     # Expected inflation 1 year out
    "CPIAUCSL",      # Actual inflation
    "WTISPLC",       # BRENT oil spot rate
    "CUSR0000SEHE",  # CPI energy prices
    "UNRATE",        # Unemployment rate
    "NROU",          # Natural rate of unemployment
    "MICH",          # U Michigan inflation expectations
]


def file_created_today(path: Path) -> bool:
    """Check if a file was created today.

    Args:
      path (pathlib.Path): Path of file
    """
    file_created_date = os.stat(path).st_birthtime
    file_created_date = datetime.datetime.utcfromtimestamp(file_created_date)
    file_created_date = file_created_date.strftime("%Y%m%d")

    today = datetime.datetime.today().strftime("%Y%m%d")

    return file_created_date == today


def acquire_fred_series(id: str) -> pd.DataFrame:
    """Query FRED API with a given series ID to get
    latest release of all possible dates for a given
    series, and save pickle file.

    Args:
      id (str): FRED series ID
    """
    data = FRED.get_series(id)
    if not isinstance(data, pd.DataFrame):
        data = data.to_frame()
    data.columns = [id]

    filename = id + ".pkl"
    with open(DATA_DIR / filename, "wb") as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return data


def acquire_fred_data() -> pd.DataFrame:
    """Query FRED API for each series, caching file.
    If cache file was created today, load from cache.
    """
    all_data = pd.DataFrame()
    for series_id in FRED_DATA:
        filename = series_id + ".pkl"
        file_path = DATA_DIR / filename

        if filename not in os.listdir(DATA_DIR):
            data = acquire_fred_series(series_id)
        elif file_created_today(file_path) is True:
            data = pd.read_pickle(file_path)
        else:
            data = acquire_fred_series(series_id)

