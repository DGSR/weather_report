import zipfile
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import List

import pandas as pd


def zip_extract_files(source: str, destination: str) -> List[str]:
    """
    Extracts from archive source to given destination
    """
    with zipfile.ZipFile(source) as zip:
        zip.extractall(destination)
        return zip.namelist()


def read_csv_files(files: List[str], max_workers: int) -> pd.DataFrame:
    """
    Read csv files in parallel and return their contents in one dataframe
    """
    df = pd.DataFrame()
    csv_read = partial(pd.read_csv, encoding='unicode_escape', header=0)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for table in pool.map(csv_read, files):
            df = df.append(table)
            df.columns = table.columns
    return df


def filter_data(table: pd.DataFrame, max_workers) -> pd.DataFrame:
    """
    return filtered DataFrame by Longitude and Latitude
    """
    df = pd.DataFrame()
    for line in table.itertuples(index=False):
        if -180 < line.Longitude < 180 and -90 < line.Latitude < 90:
            df = df.append(pd.Series(line), ignore_index=True)
    df.columns = table.columns
    return df


def top_cities(table: pd.DataFrame) -> pd.DataFrame:
    """
    return dataframe where cities have the largest number of hotels in country
    """
    df = table.groupby(['Country', 'City'], as_index=False).size()
    idx = df.groupby('Country',
                     sort=False)['size'].transform(max) == df['size']
    df = df[idx].drop_duplicates('Country')
    return table.loc[table['City'].isin(df['City'])]
