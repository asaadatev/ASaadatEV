
def data_availability(data, start_datetime=None, end_datetime=None):
    if start_datetime is not None:
        data = data.loc[data.index >= start_datetime]
    if end_datetime is not None:
        data = data.loc[data.index <= start_datetime]
    days = set([i.date() for i in data.index])
    avail_days = len(days)
    start = min(days)
    stop = max(days)
    date_range = stop - start
    total_days = date_range.days + 1
    avail_rate = avail_days / total_days
    cols = list(data.columns)
    return start, stop, total_days, avail_days, avail_rate, cols, days
