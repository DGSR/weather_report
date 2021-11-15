from api_request.weather_results import clear_elements, pairwise


def test_pairwise():
    temp = [0, 2, 4, 6, 7, 8]
    assert pairwise(temp) == [(0, 2), (4, 6), (7, 8)]


def test_clear_elements():
    temp = ['[(10.0', '11.0)', '(12.0', '13.0)]', '\n']
    temp1 = [[(11.0, 12.0), (13.0, 15.0)]]
    assert clear_elements(temp) == [(10.0, 11.0), (12.0, 13.0)]
    assert clear_elements(temp1) == [(11.0, 12.0), (13.0, 15.0)]
