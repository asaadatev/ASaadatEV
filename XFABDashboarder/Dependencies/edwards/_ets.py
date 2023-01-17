"""
Time series processing. Obsolete. To be update or deleted!

@author: Danny Sun
"""

from scipy import signal

from .data import Odbc


class Ets:
    """
    Equipment time series data object.

    Parameters
    ----------
    data : DataFrame
    database : str
    system_name : str
    system_type_id: int
    parameter_number : int
    parameter_unit_id : int
    parameter_name : str
    start_datetime : datetime
    end_datetime : datetime
    """

    def __init__(self,
                 data,
                 database=None,
                 system_name=None,
                 system_type_id=None,
                 parameter_number=None,
                 parameter_name=None,
                 parameter_unit_id=None,
                 start_datetime=None,
                 end_datetime=None):
        """Initialize `Ets` object."""
        self.data = data
        self.database = database
        self.system_name = system_name
        self.system_type_id = system_type_id
        self.parameter_number = parameter_number
        self.parameter_unit_id = parameter_unit_id
        self.parameter_name = parameter_name
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

        self._data_original = self.data

    @classmethod
    def create_from_odbc(cls,
                         database,
                         system_name,
                         parameter_number,
                         start_datetime,
                         end_datetime,
                         server=None,
                         uid=None,
                         pwd=None):
        """
        Create `Ets` time series from `Odbc` class.

        See Also
        --------
        `Odbc`
        """
        obj = Odbc.create(database=database,
                          system_name=system_name,
                          parameter_number=parameter_number,
                          start_datetime=start_datetime,
                          end_datetime=end_datetime)

        return cls(data=obj.data,
                   database=obj.database,
                   system_name=obj.system_name,
                   system_type_id=obj.system_type_id,
                   parameter_number=obj.parameter_number,
                   parameter_name=obj.parameter_name,
                   parameter_unit_id=obj.parameter_unit_id,
                   start_datetime=obj.start_datetime,
                   end_datetime=obj.end_datetime)

    def convert_unit(self):
        """Convert unit based on `parameter_unit_id`."""
        if self.parameter_unit_id == 20:     # Power W => kW
            self.data['value'] = self.data['value'] / 1000
        elif self.parameter_unit_id == 21:   # Pressure
            if self.parameter_number == 46:  # Pa => kPa (Water Pressure)
                self.data['value'] = self.data['value'] * 0.001
            else:                            # Pa => psi (Nozzle / Exhaust)
                self.data['value'] = self.data['value'] * 0.0001450377
        elif self.parameter_unit_id == 24:   # Temperature K => C
            self.data['value'] = self.data['value'] - 273.15
        elif self.parameter_unit_id == 14:   # Flow m3/s => liter/min
            self.data['value'] = self.data['value'] * 60000

    def remove_outlier(self,
                       upper_limit=None,
                       lower_limit=None,
                       inplace=False):
        """Replace outliers with NaN.

        Parameters
        ----------
        upper_limit : float, default None
            Replace values with NaN that > upper_limit
        lower_limit : float, default None
            Replace values with NaN that < lower_limit
        inplace : bool, default False
            Whether to perform the operation in place on the data.

        Returns
        -------
        DataFrame

        See Also
        --------
        pandas.DataFrame.where
        """
        if inplace:
            if upper_limit is not None:
                self.data.where(self.data <= upper_limit, inplace=inplace)
            if lower_limit is not None:
                self.data.where(self.data >= lower_limit, inplace=inplace)
            return self.data
        else:
            data = self.data.copy()
            if upper_limit is not None:
                data.where(data <= upper_limit, inplace=True)
            if lower_limit is not None:
                data.where(data >= lower_limit, inplace=True)
            return data

    def resample(self, rule='30min', closed='right',
                 label='right', func='mean', inplace=False):
        """Resample time-series data.

        Parameters
        ----------
        rule : DateOffset, Timedelta or str, default '30min'
        closed : {'right', 'left'}, default 'right'
        label : {'right', 'left'}, default 'right'
        func : function, str, list or dict
        inplace : bool, default False
            Whether to perform the operation in place on the data.

        See Also
        --------
        pandas.DataFrame.resample
        pandas.DataFrame.agg
        """
        if inplace:
            self.data = self.data.resample(rule=rule,
                                           closed=closed,
                                           label=label).agg(func=func)
            return self.data
        else:
            return (self.data.resample(rule=rule, closed=closed,
                                       label=label).agg(func=func))

    def fillna(self, value=None, method=None, inplace=False):
        """Fill NA/NaN with fixed value.

        Parameters
        ----------
        inplace : bool, default False
            Whether to perform the operation in place on the data.
        ----------

        See also pandas.DataFrame.fillna
        """
        return self.data.fillna(value=value, method=method, inplace=inplace)

    def rolling(self,
                window=10,
                min_periods=1,
                center=False,
                win_type=None,
                closed='both',
                inplace=False):
        """Smoothing - moving average.

        Parameters
        ----------
        inplace : bool, default False
            Whether to perform the operation in place on the data.
        ----------
        """
        if inplace:
            self.data = self.data.rolling(window=window,
                                          min_periods=min_periods,
                                          center=center,
                                          win_type=win_type,
                                          closed=closed).mean()
            return (self.data)
        else:
            return (self.data.rolling(window=window,
                                      min_periods=min_periods,
                                      center=center,
                                      win_type=win_type,
                                      closed=closed).mean())

    def lfilter(self, b, a, inplace=False):
        """Filter.

        Parameters
        ----------
        inplace : bool, default False
            Whether to perform the operation in place on the data.
        ----------
        """
        if inplace:
            self.data.loc[:] = signal.lfilter(b, a, self.data.values)
            return self.data
        else:
            data = self.data.copy()
            data.loc[:] = signal.lfilter(b=b, a=a, x=data.values)
            return data

    def pdm_iir(self, alpha, inplace=False):
        """Pdm IIR filter.

        Parameters
        ----------
        inplace : bool, default False
            Whether to perform the operation in place on the data.
        ----------
        """
        pass

    def reset_data(self):
        """Reset data."""
        self.data = self._data_original

    def pdm_process(self):
        """
        

        Returns
        -------
        None.

        """
        pass
