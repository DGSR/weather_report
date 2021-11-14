from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd

from api_request.api import get_request
from constant.api import WEATHER_API_KEY


def get_forecast_results(url: str, **params) -> Tuple:
    """
    get weather forecast for current and 5 days forward
    return tuple of current temp and max min temps for days
    """
    js = get_request(url, 0, **params)
    current = js['current']
    forecast_temp = js['daily']
    return (current, forecast_temp)


def get_historical_results(url: str, **params) -> List:
    """"
    get weather data 5 days before current date
    send request day-by-day, get max min temps from hours
    """
    orig = datetime.now()
    historical_temp = []
    for day in range(1, 6):
        dt = round((orig - timedelta(days=day)).timestamp())
        js_historical = get_request(url, 1, **params, **{'dt': dt})
        historical_temp.append(js_historical['history'])
    return historical_temp


def get_weather_data(table: pd.DataFrame) -> pd.DataFrame:
    """
    gets weather data for city centers from OpenWeather
    using get_forecast_results and get_historical_results
    """
    cols = ['Country', 'City', 'Longitude', 'Latitude',
            'Current', 'Forecast', 'Historical']
    df = pd.DataFrame()
    for row in table.itertuples(index=False):
        api_params = {'lat': row.Lat, 'lon': row.Lon,
                      'units': 'metric', 'appid': WEATHER_API_KEY}
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
