import os

import pandas as pd

from process.postprocess import post_process, save_results


def test_post_process():
    TEMPS = [('US', 'Arvada', 115.0, -8.0, 27.0,
             [(26.0, 28.0), (25.0, 27.0), (25.0, 27.0),
              (25.0, 27.0), (26.0, 28.0), (25.0, 28.0)],
             [(25.0, 28.0), (23.0, 30.0), (25.0, 30.0),
              (25.0, 31.0), (26.0, 28.0)]),
             ('GB', 'Paddington', -0.1, 51.5, 12.2,
             [(10.0, 13.0), (9.0, 13.0), (8.0, 12.0),
              (16.0, 10.0), (7.0, 10.0), (1.0, 10.0)],
              [(9.0, 15.0), (9.0, 14.0), (9.0, 13.0),
               (9.0, 14.0), (6.0, 12.0)])]
    cols = ['Country', 'City', 'Longitude', 'Latitude',
            'Current', 'Forecast', 'Historical']
    df = pd.DataFrame(TEMPS, columns=cols)
    res = post_process(df)
    assert res[0][0] == 'Arvada'
    assert res[1] == 'Paddington'
    assert res[2][0] == 'Paddington'
    assert res[3][0] == 'Paddington'


def test_save_results(tmpdir):
    temp_dir = tmpdir.mkdir('tmp')
    dir_0 = temp_dir.mkdir('US').mkdir('Arvada')
    dir_1 = temp_dir.mkdir('GB').mkdir('Paddington')
    TEMPS = [('US', 'Arvada'),
             ('GB', 'Paddington')]
    cols = ['Country', 'City']
    df = pd.DataFrame(TEMPS, columns=cols)
    save_results(temp_dir, df, df)
    new_list = [os.path.basename(file) for file in temp_dir.listdir()]
    assert len(temp_dir.listdir()) == 3
    assert ('center.csv' in new_list) is True
    assert os.path.basename(dir_0.listdir()[0]) == 'hotels_0.csv'
    assert os.path.basename(dir_1.listdir()[0]) == 'hotels_0.csv'
