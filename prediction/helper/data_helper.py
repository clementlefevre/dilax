
import numpy as np
from math import radians, cos, sin, asin, sqrt
from model.config_manager import Config_manager
from helper import logging

config_manager = Config_manager()


def check_missing_data(df, x_only, f_name, drop_missing=True):
    df_missing = df[df._merge == x_only]

    if df_missing.shape[0] == 0:
        logging.info("No missing data after {}".format(f_name))
    else:
        sites_missing = df_missing.groupby('idbldsite')._merge.count()

        logging.warning("after {} : No data found for :".format(f_name))
        logging.warning(sites_missing)

    if drop_missing:
        df = df[df._merge == "both"]
    df = df.drop("_merge", 1)
    return df


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(
        radians, [lon1,
                  lat1,
                  lon2,
                  lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km


def match_coordinates(datastore, df):
    datastore.training_data['latitude_closest'] =\
        datastore.training_data.latitude.apply(
        lambda x: get_nearest_coordinate(x, df.latitude))

    datastore.training_data['longitude_closest'] =\
        datastore.training_data.longitude.apply(
        lambda x: get_nearest_coordinate(x, df.longitude))

    datastore.training_data['distance_site_to_weather'] = datastore.training_data.apply(
        lambda x: haversine(x.latitude, x.longitude, x.latitude_closest, x.longitude_closest), axis=1)

    sites_weather_too_far = datastore.training_data[
        datastore.training_data.distance_site_to_weather > 20].idbldsite.unique()

    return datastore


def get_nearest_coordinate(site_coordinate, weather_coordinates):
    """Allows the matching of site with other data based on coordinates.

    Args:
        site_coordinate (TYPE): Description
        weather_coordinates (TYPE): Description

    Returns:
        TYPE: Nearest point of a list of coordinates
    """
    return min(weather_coordinates,
               key=lambda x: abs(x - site_coordinate))


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


def regularize(datastore, df, is_forecast=False):
    """Summary

    Args:
        datastore (TYPE): Description
        df (TYPE): Description
        is_forecast (bool, optional): Description

    Returns:
        TYPE: Description
    """
    df = df.reset_index()

    for col in df.columns.tolist():
        if config_manager.features[col]:
            if not is_forecast:

                df[col + "_reg"] = df.groupby('idbldsite')[col].apply(
                    lambda x: (x - x.mean()) / x.std())
                logging.info("datastore: % s: column % s has been regularized for training set",
                             datastore, col
                             )

            else:
                training_set_groupy = datastore.training_data.groupby('idbldsite')[
                    col]
                x_mean = training_set_groupy.mean()
                x_std = training_set_groupy.std()

                df[col + "_reg"] = df.apply(lambda x:
                                            (x[col] - x_mean.ix[x['idbldsite']]
                                             ) / x_std.ix[x['idbldsite']],
                                            axis=1)
                logging.info("datastore: % s: column % s has been regularized for forecasts set",
                             datastore, col
                             )
    df = df.fillna(0)
    return df
