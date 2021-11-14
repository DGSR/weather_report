import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

from constants.constants import CACHE_FILE, CACHE_FOLDER
from data.temps import TEMPS
# from process.preprocess import (filter_data, geo_center, read_csv_files,
#                                 get_top_cities)
from process.postprocess import (create_output_folders, post_process,
                                 save_results, save_stats, temp_plots)
# from api_request.weather import get_weather_data
from process.preprocess import (filter_data, geo_center, get_top_cities,
                                read_csv_files, zip_extract_files)


def parallelize_dataframe(df, func, max_workers) -> pd.DataFrame:
    """
    split dataframe to max_workers number of subsets and execute func on them
    """
    df_split = np.array_split(df, max_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        df = pd.concat(pool.map(func, df_split))
    return df


def setup_cache() -> None:
    if not os.path.exists(CACHE_FOLDER):
        os.mkdir(CACHE_FOLDER)
    if not os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'w+') as file:
            file.write('{}')


def main(source: str = 'data/hotels.zip', destination: str = 'data',
         max_workers: int = 32):
    t1 = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument("source", help="relative path to zip archive\
                                       with csv files")
    parser.add_argument("destination", help="output directory for all results")
    parser.add_argument("--max_workers", help="amount of threads to\
                                             process data")
    args = parser.parse_args()
    # source = args.source
    destination = args.destination
    max_workers = max_workers if args.max_workers else 16
    setup_cache()
    print("Extracting csv from archive")
    files = zip_extract_files(source, destination)
    # add path to files
    files = [destination+'/'+file for file in files]
    # read all csv files
    print("Reading all csv to DataFrame")
    res = read_csv_files(files, max_workers)
    print("Files shape", res.shape)

    # cast Longitude and Latitude columns to float
    res['Longitude'] = pd.to_numeric(res['Longitude'], errors='coerce')
    res['Latitude'] = pd.to_numeric(res['Latitude'], errors='coerce')
    # filter DataFrame
    # res = filter_data(res)
    res = parallelize_dataframe(res, filter_data, max_workers)
    print("Shape after filter", res.shape)
    # find top cities
    top_cities = get_top_cities(res)
    print(top_cities)
    del res
    return 0
    print("Shape after top cities", top_cities.shape)
    # find geo centers
    city_centers = geo_center(top_cities)
    print("Shape after geo_center", city_centers.shape)
    print(city_centers)
    #
    # API code
    #

    # t1 = time.time()
    # res = parallelize_dataframe(res, geopy_supply, max_workers)
    # t2 = time.time()
    # print(f"It took {t2 - t1} seconds")
    # weather_report = parallelize_dataframe(city_centers[0:100],
    #                                        get_weather_data, max_workers)

    # Reading API results from file
    weather_report = pd.DataFrame(TEMPS)
    cols = ['Country', 'City', 'Longitude', 'Latitude',
            'Current', 'Forecast', 'Historical']
    weather_report.columns = cols
    create_output_folders(destination, weather_report)
    temp_plots(destination, weather_report)
    stats = post_process(weather_report)
    print(stats)
    top_cities['Id'] = top_cities['Id'].astype('int64')
    save_results(destination, top_cities, weather_report)
    save_stats(destination, stats)
    t2 = time.time()
    print(f"It took {t2 - t1} seconds")


main()
