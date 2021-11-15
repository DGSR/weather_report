from datetime import datetime, timedelta
from typing import List, Tuple

from api_request.api import get_request


def pairwise(arr: List) -> List:
    """
    return list of tuples from list of elements
    """
    it = iter(arr)
    return list(zip(it, it))


def clear_elements(arr: List) -> List:
    """
    clear elements of list from signs and cast to float
    """
    if len(arr[0]) == 1:
        return pairwise(arr)
    temp_arr = [float(arr[0][2:])]
    for item in arr[1:-2]:
        temp = item.replace('(', '')
        temp = temp.replace(')', '')
        temp_arr.append(float(temp.strip()))
    temp_arr.append(float(arr[-2][:-2]))
    return pairwise(temp_arr)


def get_forecast_results(url: str, **params) -> Tuple:
    """
    get weather forecast for current and 5 days forward
    return tuple of current temp and max min temps for days
    """
    js = get_request(url, 0, **params)
    current = js[0]
    forecast_temp = clear_elements(js[1:])
    return (current, forecast_temp)


def get_historical_results(url: str, **params) -> List:
    """"
    get weather data 5 days before current date
    send request day-by-day, get max min temps from hours
    """
    orig = datetime.now()
    orig = orig.replace(hour=10, minute=0, second=0, microsecond=0)
    historical_temp = []
    for day in range(1, 6):
        dt = round((orig - timedelta(days=day)).timestamp())
        js = get_request(url, 1, **params, **{'dt': dt})
        historical_temp.append((js[0], js[1]))
    return [(float(item[0]), float(item[1])) for item in historical_temp]
