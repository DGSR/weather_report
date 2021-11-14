import urllib.parse
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd
import requests


def get_request(url: str, **params) -> str:
    param_api = urllib.parse.urlencode(params)
    url_api = url % param_api
    r = requests.get(url_api)
    return r.json()


def get_forecast_results(url: str, **params) -> Tuple:
    js = get_request(url, **params)
    current = js['current']['temp']
    forecast_temp = []
    for index in range(0, 6):
        forecast_temp.append((js['daily'][index]['temp']['min'],
                              js['daily'][index]['temp']['max']))
    return (current, forecast_temp)


def get_historical_results(url: str, **params) -> List:
    orig = datetime.now()
    historical_temp = []
    for day in range(1, 6):
        dt = round((orig - timedelta(days=day)).timestamp())
        js_historical = get_request(url, **params, **{'dt': dt})
        day_temps = [hourly['temp'] for hourly in js_historical['hourly']]
        historical_temp.append((min(day_temps), max(day_temps)))
    return historical_temp


def get_weather_data(table: pd.DataFrame) -> pd.DataFrame:
    """
    gets weather data for city centers from OpenWeather
    """
    api_key = ''
    cols = ['Country', 'City', 'Latitude', 'Longitude',
            'current', 'forecast', 'historical']
    df = pd.DataFrame()
    for row in table.itertuples(index=False):
        api_params = {'lat': row.Lat, 'lon': row.Lon,
                      'units': 'metric', 'appid': api_key}
        url = 'https://api.openweathermap.org/data/2.5/onecall?%s'
        exclude_param = {'exclude': 'minutely,hourly,alerts'}
        current, forecast = get_forecast_results(url, **api_params,
                                                 **exclude_param)

        url = 'https://api.openweathermap.org/data/2.5/onecall/timemachine?%s'
        historical_temps = get_historical_results(url, **api_params)

        new_row = row + (current, forecast, historical_temps)
        df = df.append(pd.Series(new_row), ignore_index=True)
    df.columns = cols
    return df
