from datetime import datetime, timedelta
from api_request.api import get_request
from typing import List, Tuple


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
