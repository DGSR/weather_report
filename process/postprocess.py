from datetime import datetime, timedelta
from operator import sub
from pathlib import Path
from typing import List, Tuple

import matplotlib.pyplot as plt
import pandas as pd

from constants.constants import STATS_FILE


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
    return Tuple of following stats:
        Day and city with max temperature
        City with maximum deviation of max temperature
        Day and city with min temperature
        Day and city with maximum deviation of
            max min temperatures
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


def save_stats(destination: str, stats: Tuple) -> None:
    """
    write stats to destination + STATS_FILE
    """
    with open(destination + STATS_FILE, 'w+') as file:
        file.writelines('Day and city with max temperature \n')
        file.writelines(stats[0][0]+' '+stats[0][1]+'\n')
        file.writelines('City with maximum deviation of max temperature\n')
        file.writelines(stats[1]+'\n')
        file.writelines('Day and city with min temperature\n')
        file.writelines(stats[2][0]+' '+stats[2][1]+'\n')
        file.writelines('Day and city with maximum deviation of \
                     max min temperatures\n')
        file.writelines(stats[3][0]+' '+stats[3][1]+'\n')


def create_output_folders(destination: str, table: pd.DataFrame) -> None:
    """
    Create folders in given destination: {output_folder}/{country}/{city}
    """
    df = table[['Country', 'City']]
    for row in df.itertuples(index=False):
        path = destination + '/' + row.Country + '/' + row.City
        Path(path).mkdir(parents=True, exist_ok=True)
