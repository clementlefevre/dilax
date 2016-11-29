
import numpy as np


def add_calendar_fields(df):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    data = df

    data['day_of_week'] = data.date.dt.dayofweek
    data['day_of_month'] = data.date.dt.day
    data['day_of_year'] = data.date.dt.dayofyear
    data['month'] = data.date.dt.month
    data['year'] = data.date.dt.year
    if 'date_time' in data.columns:
        data['hour'] = data.date_time.dt.hour

    for day in range(0, 7):
        data['day_' + str(day)] = np.where(data.day_of_week == day, 1, 0)

    for month in range(1, 13):
        data['month_' + str(month)] = np.where(data.month == month, 1, 0)

    data['public_holiday'] = np.where(data.is_public_holiday != 0, 1, 0)
    data['not_public_holiday'] = np.where(data.is_public_holiday == 0, 1, 0)
    data['school_holiday'] = np.where(data.is_school_holiday != 0, 1, 0)
    data['not_school_holiday'] = np.where(data.is_school_holiday == 0, 1, 0)
    data = data.drop(['is_public_holiday', 'is_school_holiday'], 1)

    return data
