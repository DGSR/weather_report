from api_request.api import cleanse_json


def test_cleanse_json():
    js = {
            'current': {'temp': 1},
            'daily': [{'temp': {'min': 0, 'max': 0}},
                      {'temp': {'min': 0, 'max': 0}},
                      {'temp': {'min': 0, 'max': 0}},
                      {'temp': {'min': 0, 'max': 0}},
                      {'temp': {'min': 0, 'max': 0}},
                      {'temp': {'min': 0, 'max': 0}}]
    }
    js1 = {
        'hourly': [
            {'temp': 1},
            {'temp': 2},
            {'temp': 3},
        ]
    }
    assert cleanse_json(js, 0) == [1, [(0, 0)] * 6]
    assert cleanse_json(js1, 1) == [1, 3]
