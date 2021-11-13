# import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt
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


def get_date_labels(days: int) -> List:
    """
    returns date labels for current day,
    for next arg days, for previous arg days
    """
    orig = datetime.now()
    labels = []
    for day in range(days, 0, -1):
        labels.append((orig - timedelta(days=day)).strftime('%d.%m'))
    labels.append(orig.strftime('%d.%m'))
    for day in range(1, days + 1):
        labels.append((orig + timedelta(days=day)).strftime('%d.%m'))
    return labels


def plot_temperature(labels: List, data: List, name: str,
                     destination: str, country: str, city: str) -> None:
    """
    plot data with labels titled using name, country and city
    saves plot to given destination+country+city
    """
    title = name + ' ' + country + ' ' + city
    plt.plot(labels, data)
    plt.title(title)
    plt.xlabel('Days')
    plt.ylabel('Temperature in Celcius')
    plt.savefig('data/' + country + '/' + city + '/' + title.replace(' ', '_'))
    plt.clf()


def temp_plots(destination: str, table: pd.DataFrame) -> None:
    """
    plot temperatures for countries and cities
     and save them to destination
    """
    labels = get_date_labels(5)
    for row in table.itertuples(index=False):
        mins = [el[0] for el in row.Historical]
        mins += [el[0] for el in row.Forecast]
        maxs = [el[1] for el in row.Historical]
        maxs += [el[0] for el in row.Forecast]
        plot_temperature(labels, mins, 'Minimum temperature for',
                         destination, row.Country, row.City)
        plot_temperature(labels, mins, 'Maximum temperature for',
                         destination, row.Country, row.City)


def post_process(table: pd.DataFrame) -> List:
    """
    find max max temperature, max max deviation
    find min min temperature, max deviation max min
    """
    return []


def create_output_folders(destination, table: pd.DataFrame) -> None:
    """
    Create folders in given destination: {output_folder}/{country}/{city}
    """
    df = table[['Country', 'City']]
    for row in df.itertuples(index=False):
        path = destination + '/' + row.Country + '/' + row.City
        Path(path).mkdir(parents=True, exist_ok=True)


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
    # create_output_folders(destination, weather_report)
    # temp_plots(destination, weather_report)


main()
