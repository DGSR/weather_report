import pandas as pd

from api_request.weather_results import (get_forecast_results,
                                         get_historical_results)
from constant.api import WEATHER_API_KEY


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
