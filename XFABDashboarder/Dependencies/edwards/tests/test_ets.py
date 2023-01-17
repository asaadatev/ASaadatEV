from datetime import datetime
import pandas as pd
import numpy as np

from edwards import Ets


class TestEts(object):

    def test_create_from_odbc(self):
        pass

    def test_remove_outlier(self):
        pass

    def test_resample(self):
        logtime = [datetime(2020, 5, 17, 0, 1, 1),
                   datetime(2020, 5, 17, 0, 1, 5),
                   datetime(2020, 5, 17, 0, 3, 0),
                   datetime(2020, 5, 17, 0, 3, 50),
                   datetime(2020, 5, 17, 0, 4, 30),
                   datetime(2020, 5, 17, 0, 4, 54),
                   datetime(2020, 5, 17, 0, 6, 3),
                   datetime(2020, 5, 17, 0, 6, 32),
                   datetime(2020, 5, 17, 0, 8, 43),
                   datetime(2020, 5, 17, 0, 9, 21)]
        value = [3, 4, 6, 8, 2, 6, 8, 3, 10, 8]
        data = pd.DataFrame({'logtime': logtime, 'value': value})
        ts = Ets(data.set_index('logtime'))
        ts_actual = ts.resample(rule='1min', func='mean', inplace=True)
        logtime_expected = [datetime(2020, 5, 17, 0, 2, 0),
                            datetime(2020, 5, 17, 0, 3, 0),
                            datetime(2020, 5, 17, 0, 4, 0),
                            datetime(2020, 5, 17, 0, 5, 0),
                            datetime(2020, 5, 17, 0, 6, 0),
                            datetime(2020, 5, 17, 0, 7, 0),
                            datetime(2020, 5, 17, 0, 8, 0),
                            datetime(2020, 5, 17, 0, 9, 0),
                            datetime(2020, 5, 17, 0, 10, 0)]
        value_expected = [3.5, 6, 8, 4, np.nan, 5.5, np.nan, 10, 8]
        ts_expected = pd.DataFrame(
            {'logtime': logtime_expected, 'value': value_expected})
        ts_expected = ts_expected.set_index('logtime')
        assert ts_actual.loc[:, 'value'].equals(
            other=ts_expected.loc[:, 'value'])
        # pd.testing.assert_frame_equal(ts_actual, ts_expected,
        #                               check_dtype = True,
        #                               check_index_type = True,
        #                               check_column_type = True,
        #                               check_frame_type = True,
        #                               check_freq = False)

    def test_pdm_iir(self):
        pass

    def test_rolling(self):
        pass

    def test_fillna(self):
        pass

    def test_reset_data(self):
        pass
