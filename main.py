# import time
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from operator import sub
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from data.temps import TEMPS
# from api_request.weather import get_weather_data
# from preprocess.preprocess import (zip_extract_files, read_csv_files,
#                                    filter_data, top_cities)
from preprocess.preprocess import filter_data, read_csv_files, top_cities


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


def date_format(day: int) -> str:
    """
    return formatted date days away from now
    """
    orig = datetime.now()
    return (orig + timedelta(days=day)).strftime('%d.%m')


def get_date_labels(days: int) -> List:
    """
    returns date labels for current day,
    for next arg days, for previous arg days
    """
    orig = datetime.now()
    labels = []
    for day in range(days, 0, -1):
        labels.append(date_format(-day))
    labels.append(orig.strftime('%d.%m'))
    for day in range(1, days + 1):
        labels.append(date_format(day))
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
    plt.close()
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
        plot_temperature(labels, maxs, 'Maximum temperature for',
                         destination, row.Country, row.City)


def post_process(table: pd.DataFrame) -> List:
    """
    find max max temperature, max max deviation
    find min min temperature, max deviation max min
    """
    res_value = [0, 0, 100, 0]
    res_index = [[0, 0], 0, [0, 0], [0, 0]]
    for index in table.index:
        mins = [el[0] for el in table['Historical'][index]]
        mins += [el[0] for el in table['Forecast'][index]]
        maxs = [el[1] for el in table['Historical'][index]]
        maxs += [el[1] for el in table['Forecast'][index]]

        if res_value[0] < max(maxs):
            res_value[0] = max(maxs)
            res_index[0][0] = index
            res_index[0][1] = maxs.index(max(maxs))
        if res_value[1] < max(maxs)-min(maxs):
            res_value[1] = max(maxs)-min(maxs)
            res_index[1] = index
        if res_value[2] > min(mins):
            res_value[2] = min(mins)
            res_index[2][0] = index
            res_index[2][1] = mins.index(min(mins))
        if res_value[3] < max(map(sub, maxs, mins)):
            max_min = list(map(sub, maxs, mins))
            res_value[3] = max(max_min)
            res_index[3][0] = index
            res_index[3][1] = max_min.index(max(max_min))
    return(
            (table.loc[[res_index[0][0]]]['City'].iat[0],
             date_format(res_index[0][1]-5)),
            table.loc[[res_index[1]]]['City'].iat[0],
            (table.loc[[res_index[2][0]]]['City'].iat[0],
             date_format(res_index[2][1]-5)),
            (table.loc[[res_index[3][0]]]['City'].iat[0],
             date_format(res_index[3][1]-5))
          )


def save_results(destination: str, hotels: pd.DataFrame,
                 weather: pd.DataFrame, chunk_size: int = 100) -> None:
    """
    save data about hotels in cities and countries in given destination:
    {destination}/{country}/{city}/{hotel_chunk_id.csv}
    save data about center in destination/center.csv
    """
    path = Path(destination)
    countries = [x for x in path.iterdir() if x.is_dir()]
    for country in countries:
        if not hotels['Country'].str.contains(country.name, regex=False).any():
            continue
        cities = [x for x in country.iterdir() if x.is_dir()]
        for city in cities:
            if not hotels['City'].str.contains(city.name, regex=False).any():
                continue
            selected = hotels[(hotels['Country'] == country.name) &
                              (hotels['City'] == city.name)]
            chunks = [selected[i:i+chunk_size] for i in
                      range(0, selected.shape[0], chunk_size)]
            for id, chunk in enumerate(chunks):
                chunk.to_csv(str(city) + '/hotels_{id}.csv'.format(id=id),
                             index=False)

    weather.to_csv(destination+'/center.csv', index=False)


def create_output_folders(destination: str, table: pd.DataFrame) -> None:
    """
    Create folders in given destination: {output_folder}/{country}/{city}
    """
    df = table[['Country', 'City']]
    for row in df.itertuples(index=False):
        path = destination + '/' + row.Country + '/' + row.City
        Path(path).mkdir(parents=True, exist_ok=True)


def main(source: str = 'data/hotels.zip', destination: str = 'data',
         max_workers: int = 32):
    parser = argparse.ArgumentParser()
    parser.add_argument("source", help="relative path to zip archive\
                                       with csv files")
    parser.add_argument("destination", help="output directory for all results")
    parser.add_argument("--max_workers", help="amount of threads to\
                                             process data")
    args = parser.parse_args()
    # source = args.source
    destination = args.destination
    max_workers = max_workers if args.max_workers else 32
    print("Extracting csv from archive")
    # files = zip_extract_files(source, destination)
    # # add path to files
    # files = [destination+'/'+file for file in files]
    # read all csv files
    c = 'part-00000-7b2b2c30-eb5e-4ab6-af89-28fae7bdb9e4-c000.csv'
    files = [destination+'/'+c]
    print("Reading all csv to DataFrame")
    res = read_csv_files(files, max_workers)
    print("Files shape", res.shape)

    # cast Longitude and Latitude columns to float
    res['Longitude'] = pd.to_numeric(res['Longitude'], errors='coerce')
    res['Latitude'] = pd.to_numeric(res['Latitude'], errors='coerce')
    # filter DataFrame
    res = filter_data(res, max_workers)
    print("Shape after filter", res.shape)
    # find top cities
    filtered_data = top_cities(res)
    print("Shape after top cities", filtered_data.shape)
    # find geo centers
    geo_centered = geo_center(filtered_data)
    print("Shape after geo_center", geo_centered.shape)
    #
    # API code
    #

    # t1 = time.time()
    # res = parallelize_dataframe(res, geopy_supply, max_workers)
    # t2 = time.time()
    # print(f"It took {t2 - t1} seconds")
    # city_centers = geo_center(res)
    # weather_report = parallelize_dataframe(city_centers[0:100],
    #                                        get_weather_data, max_workers)

    # Reading API results from file
    weather_report = pd.DataFrame(TEMPS)
    cols = ['Country', 'City', 'Longitude', 'Latitude',
            'Current', 'Forecast', 'Historical']
    weather_report.columns = cols
    create_output_folders(destination, weather_report)
    temp_plots(destination, weather_report)
    res0 = post_process(weather_report)
    print(res0)
    res['Id'] = res['Id'].astype('int64')
    save_results(destination, res, weather_report)


main()
