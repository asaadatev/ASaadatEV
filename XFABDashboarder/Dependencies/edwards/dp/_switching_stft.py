"""Class for computing switching-related derived parameter by STFT."""

# Authors: Danny Sun <duo.sun@edwardsvacuum.com>

import datetime
import numpy as np
import pandas as pd
from scipy import signal
import matplotlib.pyplot as plt

from . import Base
from ..utils import cal_alert_periods, iir, centroid


class SwitchingSTFT(Base):
    """
    Compute switching frequency by Short Time Fourier Transform (STFT).

    Parameters
    ----------
    upper_limit : float or int, default None
        Values greater than `upper_limit` will be replaced with NA.
    lower_limit : float or int, default None
        Values less than `lower_limit` will be replaced with NA.
    resample_rule : str or datetime.timedelta, default '1min'
        Resampling rate. See 'pandas.Series.resample' for more info.
    resample_func : str, default 'mean'
        Resampling aggregation function.
        See 'pandas.core.resample.Resampler.aggregate' for more info.
    fillna_value : float or int, default None
        Value to use to fill NA.
    fillna_method : str, default 'ffill'
        Method to use to fill NA. 'ffill': propagate last valid
        observation forward to next valid backfill.
        See 'pandas.Series.fillna' for more info.
    stft_window: str, default 'hann'
        Desired window to use. See 'scipy.signal.stft' for more info.
    stft_nperseg: int, default 1440
        Length of each segment. Default to 1440. With a resampling rate
        of 1 minutes, 1440 means 1 day.
        See 'scipy.signal.stft' for more info.
    stft_noverlap: int, default 1380
        Number of points to overlap between segments.
        If None, noverlap = nperseg // 2. Default to 1380. With a
        resampling rate of 1 minutes, 1380 means 23 hours.
        See 'scipy.signal.stft' for more info.
    stft_nfft: int, default None
        Length of the FFT used, if a zero padded FFT is desired. If None,
        the FFT length is nperseg.
        See 'scipy.signal.stft' for more info.
    stft_boundary: str, default None
        See 'scipy.signal.stft' for more info.
    stft_padded: str, default None
        See 'scipy.signal.stft' for more info.
    method: str {'fwa1', 'fwa2', 'fmax'}, default 'fwa1'
        Select feature as derived parameter. 'fwa1': weighted average
        frequency with frequency amplitudes as weights. 'fwa2': weighted
        average frequency with power amplitudes as weights.
        'fmax': frequency with maximum amplitude
    iir_alpha : float or int, default None
        IIR filter parameter.
    rolling_window : int, default 48
        Size of the moving window.
    rolling_min_periods : int, default 1
        Minimum number of observations in window required to have a value;
        otherwise, result is np.nan. If None, the size of the window.
    rolling_func : str, default 'mean'
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
    stft_ : ndarray
        STFT of data.
    graph_spectrogram_ : matplotlib.figure.Figure
        A matplotlib figure showing STFT spectrogram Assigned after call
        method `plot_features()`
    features_ : dict
        A dictionary with keys {'fwa1', 'fwa2', 'fmax'}. Values are
        corresponding features extracted from spectrum without smoothing.

    Notes
    -----
    Processing steps and their corresponding parameters

    1. Replace outliers with NA
    (Outliers are values that > `upper_limit` or < `lower_limit`).

    2. Resample/aggregate irregular time series and then fill NA with a
    value or using other methods
    (Resampling window `resample_rule`, function `resample_func`,
    `fillna_value`, `fillna_method`).

    3. Perform STFT and then extract features from spectrum
    (`stft_window`, `stft_nperseg`, `stft_noverlap`, `stft_nfft`,
    `stft_boundary`, `stft_padded`; `method` selects which following feature is
    used for calculation of derived parameter:
        - fmax: the frequency with maximum amplitude
        - fwa1: weighted average frequency with frequency amplitude  as weights
        - fwa2: weighted average frequency with power amplitude as weights).

    4. Apply iir filtering or moving average to selected feature
    (`iir_alpha`, `rolling_window`, `rolling_min_periods`, `rolling_func`).

    5. Compare derived parameter against thresholds to get alert periods
    (`threshold`, `is_upper`, `t_min_high`, `t_min_low`).

    See Also
    --------
    Base : the base class for all derived parameters.

    Examples
    --------
    See 'example/dp/switching_stft.ipynb'.
    """

    def __init__(self,
                 upper_limit: float | int = None,
                 lower_limit: float | int = None,
                 resample_rule: str | datetime.timedelta = '1min',
                 resample_func: str = 'mean',
                 fillna_value: float | int = None,
                 fillna_method: str = 'ffill',
                 stft_window: str = 'hann',
                 stft_nperseg: int = 1440,
                 stft_noverlap: int = 1380,
                 stft_nfft: int = None,
                 stft_boundary: str = None,
                 stft_padded: bool = True,
                 method: str = 'fwa1',
                 iir_alpha: float | int = None,
                 rolling_window: int = 48,
                 rolling_min_periods: int = 1,
                 rolling_func: str = 'mean',
                 threshold: dict = None,
                 is_upper: bool = True,
                 t_min_high: dict = None,
                 t_min_low: dict = None):

        if stft_noverlap is None:
            stft_noverlap = stft_nperseg / 2

        if stft_nfft is None:
            stft_nfft = stft_nperseg

        if not isinstance(resample_rule, (str, datetime.timedelta)):
            raise TypeError('resample_rule must be str or datetime.timedelta.')

        self.upper_limit = upper_limit
        self.lower_limit = lower_limit
        self.resample_rule = resample_rule
        self.resample_func = resample_func
        self.fillna_value = fillna_value
        self.fillna_method = fillna_method
        self.stft_window = stft_window
        self.stft_nperseg = stft_nperseg
        self.stft_noverlap = stft_noverlap
        self.stft_nfft = stft_nfft
        self.stft_boundary = stft_boundary
        self.stft_padded = stft_padded
        self.method = method
        self.iir_alpha = iir_alpha
        self.rolling_window = rolling_window
        self.rolling_min_periods = rolling_min_periods
        self.rolling_func = rolling_func
        self.threshold = threshold
        self.is_upper = is_upper
        self.t_min_high = t_min_high
        self.t_min_low = t_min_low

        # Additional output attributes
        self.stft_ = None
        self.graph_spectrogram_ = None
        self.features_ = {}

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

            # (1) Replace outliers with NA.
            if self.upper_limit is not None:
                self.derived_parameter_.where(
                    self.derived_parameter_ <= self.upper_limit, inplace=True)
            if self.lower_limit is not None:
                self.derived_parameter_.where(
                    self.derived_parameter_ >= self.lower_limit, inplace=True)
            if ((self.upper_limit is not None)
                    | (self.lower_limit is not None)):
                self.results_['After removing outliers'] = \
                    self.derived_parameter_.copy()

            # (2) Aggregate and fill NA with most recent non-NA value
            self.derived_parameter_ = self.derived_parameter_.resample(
                rule=self.resample_rule,
                closed='right',
                label='right',
                origin='start_day').agg(func=self.resample_func)
            self.derived_parameter_.fillna(value=self.fillna_value,
                                           method=self.fillna_method,
                                           inplace=True)
            self.results_['After aggregation & ffill'] = \
                self.derived_parameter_.copy()

            # (3a) Compute the Short Time Fourier Transform (STFT)
            if isinstance(self.resample_rule, str):
                fs = datetime.timedelta(minutes=60) / pd.Timedelta(
                    self.resample_rule)
            else:
                fs = datetime.timedelta(minutes=60) / self.resample_rule

            f, t, zxx = signal.stft(self.derived_parameter_.values,
                                    fs=fs,
                                    window=self.stft_window,
                                    nperseg=self.stft_nperseg,
                                    noverlap=self.stft_noverlap,
                                    nfft=self.stft_nfft,
                                    detrend=False,
                                    return_onesided=True,
                                    boundary=self.stft_boundary,
                                    padded=self.stft_padded,
                                    axis=-1)
            zxx = np.abs(zxx)
            dt = pd.date_range(
                self.derived_parameter_.index[0],
                periods=len(t),
                freq=self.resample_rule*(self.stft_nperseg-self.stft_noverlap))

            self.stft_ = {'f': f, 't': t, 'zxx': zxx, 'dt': dt}

            # (3b) Extract derived parameters
            self.features_['fmax'] = pd.Series(
                    data=f[np.argmax(zxx, axis=0)],
                    index=dt)
            self.features_['fwa1'] = pd.Series(
                    data=np.apply_along_axis(
                        centroid, axis=0, arr=zxx, f=f, power=1),
                    index=dt)
            self.features_['fwa2'] = pd.Series(
                    data=np.apply_along_axis(
                        centroid, axis=0, arr=zxx, f=f, power=2),
                    index=dt)
            self.derived_parameter_ = \
                self.features_.get(self.method, None).copy()
            self.results_['After extracting feature ' + self.method] = \
                self.derived_parameter_.copy()

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

            # (5) Compare with thresholds to get alert periods
            if self.threshold is not None:
                self.alert_ = cal_alert_periods(data=self.derived_parameter_,
                                                threshold=self.threshold,
                                                is_upper=self.is_upper,
                                                t_min_high=self.t_min_high,
                                                t_min_low=self.t_min_low)

    def plot_spectrogram(self):
        fig, ax = plt.subplots()
        ax.pcolormesh(self.stft_.get('dt').values,
                      self.stft_.get('f'),
                      self.stft_.get('zxx'),
                      vmin=0,  # vmax=amp,
                      shading='gouraud')
        ax.set_xlabel('')
        ax.set_ylabel('Frequency [Hz]')
        ax.set_title('STFT Magnitude')

    def plot_features(self):
        if self.features_:
            nrows = len(self.features_)
            fig, axes = plt.subplots(nrows=nrows, ncols=1)
            for i, (k, v) in enumerate(self.features_.items()):
                if isinstance(v, pd.DataFrame):
                    for j in v.columns:
                        axes[i].plot(v[j].index, v[j].values)
                    axes[i].legend(v.columns)
                else:
                    axes[i].plot(v.index, v.values)
                    axes[i].set_ylabel(v.name)
                axes[i].set_xlabel('')
                axes[i].set_title(k)
            plt.subplots_adjust(left=None, bottom=None,
                                right=None, top=None,
                                wspace=0.2, hspace=0.6)
            fig.suptitle(self.system_name,
                         horizontalalignment='left', x=0.10, y=0.95)
