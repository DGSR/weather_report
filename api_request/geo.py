from concurrent.futures import ThreadPoolExecutor
from functools import partial

import pandas as pd
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import OpenMapQuest

from constant.api import GEO_API_KEY


def geopy_supply(table: pd.DataFrame, max_workers: int) -> pd.DataFrame:
    """
    get geo coordinates by geocode.reverse for DataFrame in parallel
    return mapped results
    """
    df = table.reset_index().drop(columns='index')
    geolocator = OpenMapQuest(api_key=GEO_API_KEY)
    part_geo = partial(geolocator.reverse, language="en")
    geocode = RateLimiter(part_geo, min_delay_seconds=1/20)
    search = [(df['Latitude'][index], df['Longitude'][index])
              for index in df.index]
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        locations = list(pool.map(geocode, search))
    for index in range(len(locations)):
        address = locations[index].address.split(',')
        address = [loc.strip() for loc in address]
        if df['Name'][index] in address or df['City'][index] in address:
            df.loc[index, 'Latitude'] = locations[index].latitude
            df.loc[index, 'Longitude'] = locations[index].longitude
    return df
