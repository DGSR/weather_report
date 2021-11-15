import urllib.parse
from typing import List

import requests

from constant.constants import CACHE_FILES


def get_request(url: str, type_request: int, **params) -> dict:
    """
    get request using url and params from cache then send request
    after that call set_request_cache
    """
    param_api = urllib.parse.urlencode(params)
    url_api = url % param_api
    if (cached_data := get_cached_request(url_api, type_request)) is not None:
        return cached_data
    r = requests.get(url_api)
    res = cleanse_json(r.json(), type_request)
    set_request_cache(url_api, res, type_request)
    return res


def get_cached_request(url: str, type_request: int) -> dict:
    """
    get cached responses from json or None
    """
    with open(CACHE_FILES[type_request], 'r+') as file:
        for line in file.readlines():
            if url in line:
                return line.split(',')[1:]


def set_request_cache(url: str, js: List, type_request: int) -> None:
    """
    write response to json cache-file
    """
    with open(CACHE_FILES[type_request], 'a+') as file:
        file.write(url+',')
        for item in js:
            file.write('%s,' % item)
        file.write('\n')


def cleanse_json(js: dict, type_request: int) -> dict:
    """
    get only neccessary data depending on type_request from responses
    """
    new_res = []
    if type_request == 0:
        new_res.append(js['current']['temp'])
        list_temps = []
        for index in range(0, 6):
            list_temps.append((js['daily'][index]['temp']['min'],
                               js['daily'][index]['temp']['max']))
        new_res.append(list_temps)
    else:
        day_temps = [hourly['temp'] for hourly in js['hourly']]
        new_res.extend([min(day_temps), max(day_temps)])
    return new_res
