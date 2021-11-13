# from geopy.geocoders import Nominatim
# from geopy.extra.rate_limiter import RateLimiter
#
#
# def geopy_supply(table: pd.DataFrame) -> pd.DataFrame:
#     """
#     """
#     df = pd.DataFrame()
#     geolocator = Nominatim(user_agent="csv_data_to_stats")
#     geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)
#     for line in table.itertuples(index=False):
#         s = str(line.Latitude) + ', ' + str(line.Longitude)
#         location = geocode(s)
#         temp = line._replace(Latitude=location.latitude,
#                              Longitude=location.longitude)
#         df = df.append(pd.Series(temp), ignore_index=True)
#     df.columns = table.columns
#     return df
