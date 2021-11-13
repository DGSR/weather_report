# import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

from data.temps import TEMPS
# from api_request.weather import get_weather_data
# from preprocess.preprocess import (zip_extract_files, read_csv_files,
#                                    filter_data, top_cities)
from preprocess.preprocess import filter_data, read_csv_files


def parallelize_dataframe(df, func, max_workers) -> pd.DataFrame:
    """
    split dataframe to max_workers number of subsets and execute func on them
    """
    df_split = np.array_split(df, max_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        df = pd.concat(pool.map(func, df_split))
    return df


def geo_center(table: pd.DataFrame) -> pd.DataFrame:
    """
    returns geographical center for countries and cities
    """
    df = table.groupby(['Country', 'City'], as_index=False)\
              .agg({'Longitude': ['min', 'max'], 'Latitude': ['min', 'max']})
    df['Lon'] = (df["Longitude"]['min']+df["Longitude"]['max'])/2
    df['Lat'] = (df["Latitude"]['min']+df["Latitude"]['max'])/2
    df = df.drop(['Longitude', 'Latitude'], axis=1, level=0)
    df.columns = ['Country', 'City', 'Lon', 'Lat']
    return df


def main(source: str = 'data/hotels.zip', destination: str = 'data',
         max_workers: int = 32):
    # # extract files from archive
    # files = zip_extract_files(source, destination)
    # # add path to files
    # files = [destination+'/'+file for file in files]
    # # read all csv files
    c = 'part-00000-7b2b2c30-eb5e-4ab6-af89-28fae7bdb9e4-c000.csv'
    files = [destination+'/'+c]
    res = read_csv_files(files, max_workers)
    print("Files shape", res.shape)

    # cast Longitude and Latitude columns to float
    res['Longitude'] = pd.to_numeric(res['Longitude'], errors='coerce')
    res['Latitude'] = pd.to_numeric(res['Latitude'], errors='coerce')
    # filter DataFrame
    res = filter_data(res, max_workers)
    print("Shape after filter", res.shape)

    #
    # API code
    #
    # top_cities(res)

    # t1 = time.time()
    # res = parallelize_dataframe(res, geopy_supply, max_workers)
    # t2 = time.time()
    # print(f"It took {t2 - t1} seconds")
    # city_centers = geo_center(res)
    # weather_report = parallelize_dataframe(city_centers[0:100],
    #                                        get_weather_data, max_workers)

    # Reading API results from file
    weather_report = pd.DataFrame(TEMPS)
    cols = ['Country', 'City', 'Latitude', 'Longitude',
            'Current', 'Forecast', 'Historical']
    weather_report.columns = cols
    print(weather_report)


main()
