"""Summary
"""
import inspect
from datetime import datetime
from ..service.geocoding_API_service import create_regions_df
from ..helper import pd, logging
from ..helper.data_helper import check_missing_data, round_to_nearest_hour,\
    reindex_weather_intraday, add_idbldsite_to_weather_data
from ..service.school_holidays_service import add_school_holidays
from ..service.public_holidays_service import add_public_holidays
from ..service.conversion_service import merge_with_conversion


def merge_tables(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    logging.info("{} : start merging sites with counts...".format(datastore))
    datastore.training_data = merge_with_counts(datastore)

    logging.info("{} : start merging sites with weather...".format(datastore))
    datastore.training_data = merge_with_weather(datastore)

    logging.info(
        "{} : start merging sites with public holidays...".format(datastore))
    datastore.training_data = merge_with_public_holidays(datastore)

    logging.info(
        "{} : start merging sites with public regions...".format(datastore))
    datastore.training_data = merge_with_regions(datastore)

    logging.info(
        "{} : start merging sites with school holidays...".format(datastore))
    datastore.training_data = merge_with_school_holidays(datastore)

    logging.info(
        "{} : start merging sites with conversion...".format(datastore))
    datastore.training_data = merge_with_conversion(datastore)
    return datastore.training_data


def merge_with_counts(datastore):
    """Summary

    Args:
        datastore (TYPE): datastore

    Returns:
        TYPE: DataFrame
    """
    df_sites = datastore.db.sites
    df_counts = datastore.db.counts

    df_counts['date'] = pd.to_datetime(df_counts.timestamp.dt.date)

    if datastore.period == 'H':
        df_counts['date_time'] = df_counts['timestamp'].apply(
            lambda ts: round_to_nearest_hour(ts))

        df_counts = df_counts.groupby(['idbldsite', 'date_time']).sum()

    if datastore.period == 'D':
        df_counts = df_counts.groupby(['idbldsite', 'date']).sum()

    df_counts = df_counts.reset_index()

    if datastore.period == 'H':

        df_counts['date'] = df_counts['date_time'].apply(lambda dt:
                                                         datetime(dt.year,
                                                                  dt.month,
                                                                  dt.day))
    if datastore.period == 'D':
        df_counts['date_time'] = df_counts['date']

    df_counts = df_counts[['idbldsite',
                           'compensatedin', 'date',
                           'date_time']]

    df_counts_sites = pd.merge(df_sites, df_counts,
                               on=['idbldsite'],
                               suffixes=['sites_', 'counts'],
                               indicator=True, how='left')

    df_counts_sites = check_missing_data(df_counts_sites,
                                         "left_only",
                                         inspect.currentframe().f_code.co_name)

    return df_counts_sites[['idbldsite',
                            'compensatedin', 'date', 'date_time', 'latitude', 'longitude']]


def merge_with_weather(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """

    if datastore.period == 'H':
        return merge_with_weather_hour(datastore)

    if datastore.period == 'D':
        return merge_with_weather_day(datastore)


def merge_with_weather_hour(datastore):

    df_weather_hour = datastore.db.weather_intraday

    df_weather_day = datastore.db.weather_day

    df_weather_hour = pd.merge(df_weather_hour,
                               df_weather_day[['id', 'latitude', 'longitude']],
                               left_on='idparent',
                               right_on='id')

    df_weather_hour[['timestamp', 'temperature',
                     'cloudamount', 'weathersituation',
                     'latitude', 'longitude']]

    df_weather_hour = reindex_weather_intraday(df_weather_hour)

    df_weather_hour = df_weather_hour[~df_weather_hour.latitude.isnull()]

    df_weather_hour = add_idbldsite_to_weather_data(
        datastore, df_weather_hour)

    datastore.training_data = datastore.training_data[
        ['idbldsite',
         'latitude',
         'longitude',
         'compensatedin',
         'date', 'date_time']]

    df_weather_hour['hour'] = df_weather_hour['timestamp'].dt.hour
    df_weather_hour['date_'] = df_weather_hour['timestamp'].dt.date
    datastore.training_data[
        'hour'] = datastore.training_data['date_time'].dt.hour
    datastore.training_data[
        'date_'] = datastore.training_data['date_time'].dt.date

    df_sites_counts_weather_hour = pd.merge(datastore.training_data,
                                            df_weather_hour,
                                            left_on=['idbldsite',
                                                     'date_',
                                                     'hour'],
                                            right_on=['idbldsite',
                                                      'date_', 'hour'],
                                            how='left',
                                            suffixes=['_sites', '_weather'],
                                            indicator=True)

    df_sites_counts_weather_hour = check_missing_data(df_sites_counts_weather_hour,
                                                      "left_only",
                                                      inspect.currentframe().f_code.co_name)

    df_sites_counts_weather_hour.rename(columns={'latitude_sites': 'latitude',
                                                 'longitude_sites': 'longitude'}, inplace=True)
    df_sites_counts_weather_hour = df_sites_counts_weather_hour[['idbldsite', 'latitude', 'longitude',
                                                                 'compensatedin', 'date', 'date_time',
                                                                 'temperature', 'cloudamount', 'weathersituation']]

    # The intraday weather data have no min/max. But the weather Forecast API
    # does.
    df_sites_counts_weather_hour[
        'mintemperature'] = df_sites_counts_weather_hour.temperature
    df_sites_counts_weather_hour[
        'maxtemperature'] = df_sites_counts_weather_hour.temperature

    # df_sites_counts_weather_day.to_csv('data/test_merge_weather_hour.csv')
    return df_sites_counts_weather_hour


def merge_with_weather_day(datastore):

    df_weather_day = datastore.db.weather_day

    df_weather_day = df_weather_day[['maxtemperature', 'mintemperature',
                                     'weathersituation',
                                     'cloudamount',
                                     'day',
                                     'latitude', 'longitude']]
    df_weather_day.rename(
        columns={'day': 'date'}, inplace=True)

    df_weather_day = add_idbldsite_to_weather_data(datastore, df_weather_day)
    datastore.training_data = datastore.training_data[
        ['idbldsite',
         'latitude',
         'longitude',
         'compensatedin',
         'date', 'date_time']]

    df_sites_counts_weather_day = pd.merge(datastore.training_data,
                                           df_weather_day,
                                           left_on=['idbldsite',
                                                    'date'],
                                           right_on=['idbldsite', 'date'],
                                           how='left',
                                           suffixes=['_sites', '_weather'],
                                           indicator=True)

    df_sites_counts_weather_day = check_missing_data(df_sites_counts_weather_day,
                                                     "left_only",
                                                     inspect.currentframe().f_code.co_name)

    df_sites_counts_weather_day = df_sites_counts_weather_day[['idbldsite', 'latitude_sites', 'longitude_sites', 'compensatedin',
                                                               'date', 'date_time', 'maxtemperature', 'mintemperature',
                                                               'weathersituation', 'cloudamount']]

    df_sites_counts_weather_day.rename(columns={'latitude_sites': 'latitude',
                                                'longitude_sites': 'longitude'}, inplace=True)

    return df_sites_counts_weather_day


def merge_with_public_holidays(datastore):

    datastore.training_data = add_public_holidays(datastore.training_data,
                                                  datastore.public_holidays)

    return datastore.training_data


def merge_with_regions(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    df_regions = create_regions_df(datastore)
    df_with_regions = pd.merge(
        datastore.training_data, df_regions, on='idbldsite', how='left')

    df_with_regions = df_with_regions.drop(['latitude',
                                            'longitude', 'region'], 1)

    return df_with_regions


def merge_with_school_holidays(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    return add_school_holidays(datastore.training_data)
