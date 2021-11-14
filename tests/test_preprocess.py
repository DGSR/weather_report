# from unittest.mock import patch

# from process.preprocess import filter_data
# import pandas  # noqa: F401
# from pandas._testing import assert_frame_equal


# def test_filter_data():
#     cols = ['Longitude', 'Latitude']
#     df = pandas.DataFrame([[1.0, 2.0], [3.0, 4.0]], columns=cols)
#     df_res = filter_data(df)
#     df1 = pandas.DataFrame([[-182.0, 3.0], [5.0, 6.0]],
#                            columns=cols)
#     df1_0 = pandas.DataFrame([[5.0, 6.0]], columns=cols)
#     df2 = pandas.DataFrame([[-112.0, 3.0], [5.0, 91.0]],
#                            columns=cols)
#     df2_0 = pandas.DataFrame([[-112.0, 3.0]], columns=cols)
#     df3 = pandas.DataFrame([[181.0, 2.0], [9.0, -90.0]], columns=cols)
#     df3_0 = pandas.DataFrame(columns=['A', 'B'])
#     assert assert_frame_equal(df_res, df, check_dtype=False,)
#     assert assert_frame_equal(filter_data(df1), df1_0, check_dtype=False)
#     assert assert_frame_equal(filter_data(df2), df2_0, check_dtype=False)
#     assert assert_frame_equal(filter_data(df3), df3_0, check_dtype=False)
