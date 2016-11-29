
import numpy as np
import pandas as pd
from math import radians, cos, sin, asin, sqrt
import datetime
from ..model.config_manager import Config_manager
from . import logging

config_manager = Config_manager()


def check_missing_data(df, x_only, f_name, drop_missing=True):
    df_missing = df[df._merge == x_only]

    if df_missing.shape[0] == 0:
        logging.info("No missing data after {}.".format(f_name))
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


def match_coordinates(df0, df):

    df0['latitude_closest'] = df0.latitude.apply(
        lambda x: get_nearest_coordinate(x, df.latitude))

    df0['longitude_closest'] = df0.longitude.apply(
        lambda x: get_nearest_coordinate(x, df.longitude))

    df0['distance_site_to_weather'] = df0.apply(
        lambda x: haversine(x.latitude, x.longitude, x.latitude_closest, x.longitude_closest), axis=1)

    try:
        sites_weather_too_far = df0[
            df0.distance_site_to_weather > 20].idbldsite.unique()

        logging.warning("Those sites are too far from weather coordinate for matching :{}".format(
            sites_weather_too_far))
    except AttributeError as e:
        logging.info("All sites are less than 20km from weather data.")

    return df0


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

    df = df[~df.idbldsite.isin(datastore.no_weatherstore_sites)]

    for col in df.columns.tolist():

        if config_manager.features[col].regularize:
            if not is_forecast:

                df[col + "_reg"] = df.groupby('idbldsite')[col].apply(
                    lambda x: (x - x.mean()) / x.std())
                logging.info("{0} : {1} has been regularized for training set".format(
                             datastore, col))

            else:
                training_set_groupy = datastore.training_data.groupby('idbldsite')[
                    col]
                x_mean = training_set_groupy.mean()

                x_std = training_set_groupy.std()

                def standardize(row):

                    if row['idbldsite'] in x_mean.index and row['idbldsite'] in x_std.index:

                        if x_std.ix[row['idbldsite']] != 0:
                            standardized = (row[col] - x_mean.ix[row['idbldsite']]
                                            ) / x_std.ix[row['idbldsite']]
                        else:
                            standardized = 0
                    else:

                        standardized = 0
                        logging.error(
                            "not weather data in training set found for {}".format(row['idbldsite']))

                    return standardized

                df[col + "_reg"] = df.apply(standardize,
                                            axis=1)

                logging.info("{0} : {1} has been regularized for forecasts set".format(
                             datastore, col
                             ))
    df = df.fillna(0)

    return df


def round_to_nearest_hour(ts):
    dt = datetime.datetime(ts.year, ts.month, ts.day, ts.hour)
    minutes_to_hour = round(float(ts.minute) / 60, 0)
    dt = dt + datetime.timedelta(hours=minutes_to_hour)
    return dt
