import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

from api_request.geo import geopy_supply
from api_request.weather import get_weather_data
from constant.constants import CACHE_FILES, CACHE_FOLDER
from process.postprocess import (create_output_folders, post_process,
                                 save_results, save_stats, temp_plots)
from process.preprocess import (filter_data, geo_center, get_top_cities,
                                read_csv_files, zip_extract_files)


def parallelize_dataframe(df, func, max_workers) -> pd.DataFrame:
    """
    split dataframe to max_workers number of subsets and execute func on them
    """
    df_split = np.array_split(df, max_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        df = pd.concat(pool.map(func, df_split), ignore_index=True)
    return df


def setup_cache() -> None:
    """
    Create CACHE_FOLDER folder and CACHE_FILE file for caching
    """
    if not os.path.exists(CACHE_FOLDER):
        os.mkdir(CACHE_FOLDER)
    for file in CACHE_FILES:
        if not os.path.exists(file):
            with open(file, 'w+') as file:
                file.write(' ')


def main():
    t1 = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument("source", help="relative path to zip archive\
                                       with csv files")
    parser.add_argument("destination", help="output directory for all results")
    parser.add_argument("--max_workers", help="amount of threads to\
                                             process data")
    args = parser.parse_args()
    source = args.source
    destination = args.destination
    max_workers = args.max_workers if args.max_workers else 16

    setup_cache()
    print('Starting Preprocess')
    files = zip_extract_files(source, destination)
    files = [destination+'/'+file for file in files]
    res = read_csv_files(files, max_workers)

    res['Longitude'] = pd.to_numeric(res['Longitude'], errors='coerce')
    res['Latitude'] = pd.to_numeric(res['Latitude'], errors='coerce')

    res = parallelize_dataframe(res, filter_data, max_workers)
    hotels_top_city = get_top_cities(res)
    del res
    print('Finishing Preprocess')
    print('Getting GeoData')
    geo_data = geopy_supply(hotels_top_city, max_workers)
    print('GeoData Received')
    city_centers = geo_center(geo_data)
    print('Getting Weather Data')
    weather_report = parallelize_dataframe(city_centers,
                                           get_weather_data, max_workers)
    print('Got Weather Data, Starting Postprocess')
    create_output_folders(destination, weather_report)
    temp_plots(destination, weather_report)
    stats = post_process(weather_report)
    geo_data['Id'] = geo_data['Id'].astype('int64')
    save_results(destination, geo_data, weather_report)
    save_stats(destination, stats)
    t2 = time.time()
    print('Finishing Postprocess')
    print(f"It took {t2 - t1} seconds")


main()
