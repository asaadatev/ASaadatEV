import datetime
import pandas as pd
import copy


def compress_time(data: pd.DataFrame | pd.Series,
                  ratio: int | float,
                  origin: datetime.datetime | datetime.date | None = None) \
        -> pd.DataFrame | pd.Series:
    """Compress or decompress the timestamps of time-series data.

    Parameters
    ----------
    data : pd.DataFrame or pd.Series
        Time-series data, which must have a datetime-like index.
    ratio : int or float
        The ratio of original datetime-like index to compressed datetime-like
        index. To compress time, use a `ratio` value > 1. To decompress time,
        use a `ratio` value < 1.
    origin : datetime.datetime, datetime.date, or None, optional
        The timestamp used as origin for time compression and decompression.
        Default None. If None, the first index of `data` will be used.

    Returns
    -------
    pd.DataFrame or pd.Series
    """

    if not (isinstance(data, pd.DataFrame) | isinstance(data, pd.Series)):
        raise TypeError('`data` must be pd.DataFrame or pd.Series type!')

    if not isinstance(data.index, pd.DatetimeIndex):
        raise TypeError('`data.index` must be datetime type!')

    if origin is None:
        origin = data.index.values[0]

    result = copy.copy(data)

    result.index = origin + (result.index - origin) / ratio

    return result
