"""Class for monitoring inverter speed dips."""

# Authors: Danny Sun <duo.sun@edwardsvacuum.com>

import pandas as pd

from ..dp import Base
from ..utils import cal_alert_periods
from ..utils import iir


class InverterSpeed(Base):
    """
    Monitor Inverter Speed Dips.

    Parameters
    ----------
    normal_level: float or str, default 100
        Normal inverter speed level.
    resample_rule : str, default '60min'
        Resampling rate. See 'pandas.Series.resample' for more info.
    resample_func : str, default 'min'
        Resampling aggregation function.
        See 'pandas.core.resample.Resampler.aggregate' for more info.
    upper_limit : float or int, default None
        Values greater than `upper_limit` will be replaced with NA.
    lower_limit : float or int, default 20
        Values less than `lower_limit` will be replaced with NA.
    fillna_value : float or int, default 100
        Value to use to fill NA.
    fillna_method : str, default None
        Method to use to fill NA. 'ffill': propagate last valid
        observation forward to next valid backfill.
        See 'pandas.Series.fillna' for more info.
    iir_alpha : float or int, default None
        IIR filter parameter.
    rolling_window : int, default 24
        Size of the moving window.
    rolling_min_periods : int, default 12
        Minimum number of observations in window required to have a value;
        otherwise, result is np.nan. If None, the size of the window.
    rolling_func : str, default 'max'
        Aggregation function after performing rolling.
        See 'pandas.core.resample.Resampler.aggregate' for more info.
    threshold : dict, default None
        Keys are alert levels, while values are corresponding thresholds.
    is_upper : bool, default True
        If True, data that are greater than or equal to `threshold`
        are regarded as positive.
        If False, data that are less than or equal to `threshold`
        are regarded as positive.
    t_min_high : dict, default None
        Keys are alert levels, while values are corresponding
        minimum high/positive time to trigger multi-level alerts.
    t_min_low : dict, default None
        Keys are alert levels, while values are
        minimum low/negative time to clear multi-level alerts.

    Attributes
    ----------
    derived_parameter_ : pd.Series
        Derived parameter.
    alert_ : dict
        A dictionary contains alert related info and results. Key-value
        paris are as follows.
        'levels' : list of str
            Alert levels, for example ['advisory', 'warning', 'alarm'],
            determined by the keys of the input parameter `threshold`.
        'periods' : dict
            Merged alert periods. Original alerts of different levels are
            computed independently and therefore can have overlapping alert
            periods. 'periods' gives merged periods, which indicate only the
            highest level of alert at each time point.
            {`levels[0]`: pd.DataFrame with columns 'start' and 'end',
             `levels[1]`: pd.DataFrame with columns 'start' and 'end',
             ...}
        'periods_unmerged' : dict
            Unmerged alert periods.
            {`levels[0]`: pd.DataFrame with columns 'start' and 'end',
             `levels[1]`: pd.DataFrame with columns 'start' and 'end',
             ...}
        'signal' : pd.Series
            Alert signal which indicates the highest level of alert at each
            time point.
        'sampling_rate' : pd.Timedelta
            Sampling rate of time series `derived_parameter_`, also known as
            'time interval' or 'frequency'.
    results_ : dict
         A dictionary contains original data, key intermediate processing
         results, as well as derived parameter.
    graph_derived_parameter_ : matplotlib.figure.Figure
        A matplotlib figure showing derived parameter. Assigned after call
        method `plot_derived_parameter()`
    graph_results_ : matplotlib.figure.Figure
        A matplotlib figure showing original data, key intermediate processing
        results, derived parameter, and alert signal. Assigned after call
        method `plot_results()`
    vis_as_df_ : pd.DataFrame
        A pd.DataFrame used to store `results_` and also plot-related info
        such as alerts and thresholds for post-processing and further
        visualization. Assigned after call method `save_vis_as_df()`.

    Notes
    -----
    Processing steps and their corresponding parameters

    1. Aggregate and fill NA with most recent non-NA value
    (Resampling window `resample_rule`, function `resample_func`).

    2. Replace outliers with a fixed value or the most recent non-outlier value
    (Outliers are values that > `upper_limit` or < `lower_limit`; fixed
    value `fillna_value`, most recent non-outlier 'fillna_method` = 'ffill').

    3. Convert speed to dip

    4. Apply iir filtering or moving average
    (`iir_alpha`, `rolling_window`, `rolling_min_periods`, `rolling_func`).

    5. Compare derived parameter against thresholds to get alert periods
    (`threshold`, `is_upper`, `t_min_high`, `t_min_low`)

    See Also
    --------
    Base : the base class for all derived parameters.

    Examples
    --------
    TODO
    """

    def __init__(self,
                 normal_level: float | int = 100,
                 resample_rule: str = '60min',
                 resample_func: str = 'min',
                 upper_limit: float | int = None,
                 lower_limit: float | int = 20,
                 fillna_value: float | int = 100,
                 fillna_method: str = None,
                 iir_alpha: float | int = None,
                 rolling_window: int = 24,
                 rolling_min_periods: int = 12,
                 rolling_func: str = 'max',
                 threshold: dict = None,
                 is_upper: bool = True,
                 t_min_high: dict = None,
                 t_min_low: dict = None):

        self.normal_level = normal_level
        self.resample_rule = resample_rule
        self.resample_func = resample_func
        self.upper_limit = upper_limit
        self.lower_limit = lower_limit
        self.fillna_value = fillna_value
        self.fillna_method = fillna_method
        self.iir_alpha = iir_alpha
        self.rolling_window = rolling_window
        self.rolling_min_periods = rolling_min_periods
        self.rolling_func = rolling_func
        self.threshold = threshold
        self.is_upper = is_upper
        self.t_min_high = t_min_high
        self.t_min_low = t_min_low

        super(Base, self).__init__()

    def process(self, src):
        """Process data.

        Parameters
        ----------
        src: pd.Series, dict, or object
            If `src` is dict/object, it must have key/attribute 'data',
            and it may have key/attribute 'system_name' and 'parameter_name'.
        """

        # Reset all output attributes.
        self.reset()

        # Extract data, system_name, and parameter_name from `src`.
        self.parse_src(src)

        if self.data is not None:

            if not isinstance(self.data, pd.Series):
                raise TypeError('Data must be pandas.Series.')

            self.results_['Original'] = self.data.copy()
            self.derived_parameter_ = self.data.copy()

            # (1) Aggregate and fill NA with most recent non-NA value
            self.derived_parameter_ = self.derived_parameter_.resample(
                rule=self.resample_rule,
                closed='right',
                label='right',
                origin='start_day').agg(func=self.resample_func)
            self.derived_parameter_.fillna(method='ffill', inplace=True)
            self.results_['After aggregation & ffill'] = \
                self.derived_parameter_.copy()

            # (2) Replace outliers with a fixed value
            # or most recent non-outlier value
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
                self.results_['After replacing outlier'] = \
                    self.derived_parameter_.copy()

            # (3) Convert speed to dip
            self.derived_parameter_ = \
                self.normal_level - self.derived_parameter_

            # (4) Apply iir filtering or moving average.
            if self.iir_alpha is not None:
                self.derived_parameter_.loc[:] = iir(
                    x=self.derived_parameter_.values, alpha=self.iir_alpha)
                self.results_['After smoothing'] = \
                    self.derived_parameter_.copy()
            elif self.rolling_window is not None:
                self.derived_parameter_ = self.derived_parameter_.rolling(
                    window=self.rolling_window,
                    min_periods=self.rolling_min_periods,
                    center=False,
                    win_type=None,
                    closed='right').agg(func=self.rolling_func)
                self.results_['After smoothing'] = \
                    self.derived_parameter_.copy()

            # (5) Compare against thresholds to get alert periods
            if self.threshold is not None:
                self.alert_ = cal_alert_periods(data=self.derived_parameter_,
                                                threshold=self.threshold,
                                                is_upper=self.is_upper,
                                                t_min_high=self.t_min_high,
                                                t_min_low=self.t_min_low)
