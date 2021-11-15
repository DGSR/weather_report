import zipfile
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import List

import pandas as pd


def zip_extract_files(source: str, destination: str) -> List[str]:
    """
    extract from archive source output to destination
    Return filenames
    """
    with zipfile.ZipFile(source) as zip:
        zip.extractall(destination)
        return zip.namelist()


def read_csv_files(files: List[str], max_workers: int) -> pd.DataFrame:
    """
    read csv files in parallel and return their contents in one dataframe
    """
    df = pd.DataFrame()
    csv_read = partial(pd.read_csv, encoding='unicode_escape', header=0)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for table in pool.map(csv_read, files):
            df = df.append(table)
            df.columns = table.columns
    return df


def filter_data(table: pd.DataFrame) -> pd.DataFrame:
    """
    return filtered DataFrame by Longitude and Latitude
    """
    df = pd.DataFrame()
    for line in table.itertuples(index=False):
        if -180 < line.Longitude < 180 and -90 < line.Latitude < 90:
            df = df.append(pd.Series(line), ignore_index=True)
    if not df.empty:
        df.columns = table.columns
    return df


def get_top_cities(table: pd.DataFrame) -> pd.DataFrame:
    """
    filter dataframe where cities have the largest number of hotels in country
    """
    df = table.groupby(['Country', 'City'], as_index=False).size()
    idx = df.groupby('Country',
                     sort=False)['size'].transform(max) == df['size']
    df = df[idx].drop_duplicates('Country')
    return table.loc[table['City'].isin(df['City'])]


def geo_center(table: pd.DataFrame) -> pd.DataFrame:
    """
    return geographical center for countries and cities
    """
    df = table.groupby(['Country', 'City'], as_index=False)\
              .agg({'Longitude': ['min', 'max'], 'Latitude': ['min', 'max']})
    df['Lon'] = (df["Longitude"]['min']+df["Longitude"]['max'])/2
    df['Lat'] = (df["Latitude"]['min']+df["Latitude"]['max'])/2
    df = df.drop(['Longitude', 'Latitude'], axis=1, level=0)
    df.columns = ['Country', 'City', 'Lon', 'Lat']
    return df
