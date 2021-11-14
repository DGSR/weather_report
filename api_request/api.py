import json
import urllib.parse

import requests

from constants.constants import CACHE_FILE


def get_request(url: str, **params) -> dict:
    """
    get request using url and params from cache then send request
    after that call set_request_cache
    """
    param_api = urllib.parse.urlencode(params)
    url_api = url % param_api
    if (cached_data := get_cached_request(url_api)) is not None:
        return cached_data
    r = requests.get(url_api)
    res = r.json()
    set_request_cache({url_api: res})
    return r.json()


def get_cached_request(url: str) -> dict:
    """
    get cached responses from json or None
    """
    with open(CACHE_FILE, 'r+') as file:
        data = json.load(file)
        return data.get(url, None)


def set_request_cache(js: dict) -> None:
    """
    write response to json cache-file
    """
    with open(CACHE_FILE, "r+") as file:
        data = json.load(file)
        file.seek(0)
        json.dump({**data, **js}, file)
