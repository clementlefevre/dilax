"""Summary
"""
import numpy as np


def round_coordinate(coordinate):
    """Round a coordinate in order to match with other coordinate.


    Args:
        coordinate (int): latitude/longgitude

    Returns:
        TYPE: rounded coordinate
    """
    return str(round(coordinate, 2))


def round_coordinates(df):
    """Summary

    Args:
        df (TYPE): Description

    Returns:
        TYPE: Description
    """
    rounded_latitude = df.latitude.apply(
        round_coordinate)
    rounded_longitude = df.longitude.apply(round_coordinate)
    df['coord'] = rounded_latitude + ";" + rounded_longitude
    return df


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

    for day in range(0, 7):
        data['day_' + str(day)] = np.where(data.day_of_week == day, 1, 0)

    for month in range(1, 13):
        data['month_' + str(month)] = np.where(data.month == month, 1, 0)

    data['public_holiday'] = np.where(data.is_public_holiday != 0, 1, 0)
    data['not_public_holiday'] = np.where(data.is_public_holiday == 0, 1, 0)
    data['school_holiday'] = np.where(data.is_school_holiday != 0, 1, 0)
    data['not_school_holiday'] = np.where(data.is_school_holiday == 0, 1, 0)
    data.drop(['is_public_holiday', 'is_school_holiday'], 1)

    return data


def get_sites_dict(df_sites):
    """This is useful to iterate over sites
    and retrieve the weather data from the API.

    Args:
        df_sites (TYPE): Description

    Returns:
        TYPE: Description
    """
    sites = df_sites
    sites = sites[['idbldsite', 'sname', 'latitude', 'longitude']]
    sites = sites.set_index('idbldsite')
    sites_dict = sites[['sname', 'latitude',
                        'longitude']].T.apply(tuple).to_dict()
    return sites_dict


def regularize_forecasts():
    pass


def regularize(datastore, df, is_forecast=False):

    df = df.reset_index()
    print df.columns.tolist()
    for col in df.columns.tolist():
        if datastore.config.features[col]:
            if not is_forecast:

                df[col + "_reg"] = df.groupby('idbldsite')[col].transform(
                    lambda x: (x - x.mean()) / x.std())

            else:
                x_mean = datastore.training_data.groupby('idbldsite')[
                    col].mean()

                x_std = datastore.training_data.groupby('idbldsite')[
                    col].std()
                df[col + "_reg"] = df.apply(lambda x:
                                            (x[col] - x_mean.ix[x['idbldsite']]
                                             ) / x_std.ix[x['idbldsite']],
                                            axis=1)

    df = df.fillna(0)
    return df
