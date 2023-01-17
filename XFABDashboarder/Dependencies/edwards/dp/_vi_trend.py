"""Class for computing trend-related derived parameter."""

# Authors: Danny Sun <duo.sun@edwardsvacuum.com>,
#          Dennis Hou <duanyang.hou@edwardsvacuum.com>

import copy
import pandas as pd
from ._base import Base
from ..utils._cal_alert_periods import cal_alert_periods


class ViTrend(Base):
    """ VI trend """

    def __init__(self,
                 resample_rule_1='30min',
                 resample_rule_2='1D',
                 resample_func='mean',
                 upper_limit=None,
                 lower_limit=None,
                 fillna_value=None,
                 fillna_method='ffill',
                 iir_alpha=None,
                 rolling_window=48,
                 rolling_min_periods=1,
                 threshold=None,
                 is_upper=True,
                 include_last=False,
                 t_min_high=None,
                 t_min_low=None):

        # Configurations
        self.resample_rule_1 = resample_rule_1
        self.resample_rule_2 = resample_rule_2
        self.resample_func = resample_func
        self.upper_limit = upper_limit
        self.lower_limit = lower_limit
        self.fillna_value = fillna_value
        self.fillna_method = fillna_method
        self.iir_alpha = iir_alpha
        self.rolling_window = rolling_window
        self.rolling_min_periods = rolling_min_periods
        self.threshold = threshold
        self.is_upper = is_upper
        self.include_last = include_last
        self.t_min_high = t_min_high
        self.t_min_low = t_min_low

        super(Base, self).__init__()

    def process(self, src):
        """
        Note
        --------
        Processing steps
        (1) Aggregate and fill NA with most recent non-NA value
        (2) Replace outliers with a fixed value or
            most recent non-outlier value (optional)
        (3) Apply moving average or iir filter
        (4) Calculate the delta
        (5) Compare with thresholds to get alert periods
        """

        self.reset()
        self.parse_src(src)

        if self.data is not None:

            self.results_['original'] = self.data.copy()

            # (1) Aggregate and fill NA with most recent non-NA value
            self.derived_parameter_ = copy.deepcopy(self.data)
            self.derived_parameter_ = self.derived_parameter_.resample(
                rule=self.resample_rule_1,
                closed='right',
                label='right',
                origin='start_day').agg(func=self.resample_func)
            self.derived_parameter_.fillna(method='ffill', inplace=True)
            self.results_['after aggregation & ffill'] = copy.deepcopy(
                self.derived_parameter_)

            # (2) Replace outliers with a fixed value or
            # most recent non-outlier value (optional)
            if self.upper_limit is not None:
                self.derived_parameter_.where(
                    self.derived_parameter_ <= self.upper_limit, inplace=True)
            if self.lower_limit is not None:
                self.derived_parameter_.where(
                    self.derived_parameter_ >= self.lower_limit, inplace=True)
            if (self.upper_limit is not None) | (self.lower_limit is not None):
                self.derived_parameter_.fillna(value=self.fillna_value,
                                               method=self.fillna_method,
                                               inplace=True)
                self.results_['after outlier removal & replacement'] = \
                    copy.deepcopy(self.derived_parameter_)

            # (3) Apply moving average or iir filter
            if self.iir_alpha is not None:
                self.derived_parameter_.loc[:] = self.iir(
                    x=self.derived_parameter_.values, alpha=self.iir_alpha)
            else:
                self.derived_parameter_ = self.derived_parameter_.rolling(
                    window=self.rolling_window,
                    min_periods=self.rolling_min_periods,
                    center=False,
                    win_type=None,
                    closed='right').mean()
            self.results_['after smoothing'] = self.derived_parameter_.copy()

            # (4) Calculate delta
            self.derived_parameter_ = self.derived_parameter_.resample(
                rule=self.resample_rule_2,
                closed='right',
                label='right',
                origin='start_day').agg(func=self.resample_func)
            self.derived_parameter_ = self.derived_parameter_.diff()
            self.derived_parameter_ = pd.Series(index=self.derived_parameter_.index,
                                                data=[1.0 if i>0 else 0 for i in self.derived_parameter_.fillna(0)])

            self.derived_parameter_ = self.derived_parameter_.rolling('7D').sum()
            self.results_['after differencing'] = self.derived_parameter_.copy()

            # (4) Compare with thresholds to get alert periods
            if self.threshold is not None:
                self.alert_ = cal_alert_periods(
                    data=self.derived_parameter_, threshold=self.threshold,
                    is_upper=self.is_upper, include_last=self.include_last,
                    t_min_high=self.t_min_high, t_min_low=self.t_min_low)



    @staticmethod
    def iir(x, alpha):
        """ IIR filter """

        y = copy.deepcopy(x)
        if len(x) > 1:
            y[0] = x[0]
            for i in range(1, len(x)):
                y[i] = (1 - alpha) * y[i - 1] + alpha * x[i]
        # y, _ = signal.lfilter(b=[alpha],
        #                       a=[1, alpha - 1],
        #                       x=x,
        #                       zi=[x[0] * (1 - alpha)])
        return y
