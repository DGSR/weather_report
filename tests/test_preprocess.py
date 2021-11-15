import pandas  # noqa: F401
from numpy import array_equal

from process.preprocess import filter_data, geo_center, get_top_cities


def test_filter_data():
    cols = ['Longitude', 'Latitude']
    df = pandas.DataFrame([[1.0, 2.0], [3.0, 4.0]], columns=cols)
    df1 = pandas.DataFrame([[-182.0, 3.0], [5.0, 6.0]],
                           columns=cols)
    df1_0 = pandas.DataFrame([[5.0, 6.0]], columns=cols)
    df2 = pandas.DataFrame([[-112.0, 3.0], [5.0, 91.0]],
                           columns=cols)
    df2_0 = pandas.DataFrame([[-112.0, 3.0]], columns=cols)
    assert filter_data(df).equals(df) is True
    assert filter_data(df1).equals(df1_0) is True
    assert filter_data(df2).equals(df2_0) is True


def test_get_top_cities():
    cols = ['Country', 'City']
    TEMPS = [('US', 'Arvada'),
             ('GB', 'London'),
             ('GB', 'Paddington'),
             ('GB', 'Paddington')]
    res = [('US', 'Arvada'),
           ('GB', 'Paddington'),
           ('GB', 'Paddington')]
    df = pandas.DataFrame(TEMPS, columns=cols)
    df_res = pandas.DataFrame(res, columns=cols)
    df_func = get_top_cities(df)
    assert array_equal(df_func, df_res) is True


def test_geo_center():
    cols = ['Country', 'City', 'Longitude', 'Latitude']
    TEMPS = [('US', 'Arvada', 1.0, 4.0),
             ('US', 'Arvada', 2.0, 7.0),
             ('US', 'Arvada', 3.0, 8.0)]
    TEMPS1 = [('US', 'Arvada', 2.0, 6.0)]
    df = pandas.DataFrame(TEMPS, columns=cols)
    df_res = pandas.DataFrame(TEMPS1, columns=cols)
    assert array_equal(geo_center(df), df_res) is True
