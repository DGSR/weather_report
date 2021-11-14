import json
import urllib.parse

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
    set_request_cache({url_api: res}, type_request)
    return res


def get_cached_request(url: str, type_request: int) -> dict:
    """
    get cached responses from json or None
    """
    with open(CACHE_FILES[type_request], 'r+') as file:
        data = json.load(file)
        return data.get(url, None)


def set_request_cache(js: dict, type_request: int) -> None:
    """
    write response to json cache-file
    """
    with open(CACHE_FILES[type_request], "r+") as file:
        data = json.load(file)
        file.seek(0)
        json.dump({**data, **js}, file)


def cleanse_json(js: dict, type_request: int) -> dict:
    """
    get only neccessary data depending on type_request from responses
    """
    new_js = {}
    if type_request == 0:
        new_js['current'] = js['current']['temp']
        list_temps = []
        for index in range(0, 6):
            list_temps.append((js['daily'][index]['temp']['min'],
                               js['daily'][index]['temp']['max']))
        new_js['daily'] = list_temps
    if type_request == 1:
        day_temps = [hourly['temp'] for hourly in js['hourly']]
        new_js['history'] = (min(day_temps), max(day_temps))
    if type_request == 2:
        pass
    return new_js
