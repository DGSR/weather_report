from unittest.mock import patch

from freezegun import freeze_time

from api_request.weather import get_forecast_results, get_historical_results


def test_get_forecast_results():
    js = {
            'current': 1,
            'daily': [(0, 0),
                      (0, 0),
                      (0, 0),
                      (0, 0),
                      (0, 0),
                      (0, 0)]
    }
    with patch('api_request.weather.get_request', return_value=js):
        assert get_forecast_results('') == (1, [(0, 0)] * 6)


def test_get_historical_results():
    js = {
        'history': (1, 3)
    }

    with patch('api_request.weather.get_request', return_value=js), \
         freeze_time('2021-11-13'):
        assert get_historical_results('') == [(1, 3)] * 5
