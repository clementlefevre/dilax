"""Summary
"""
import inspect
from datetime import datetime
from data_helper import match_coordinates
from service.geocoding_API_service import create_regions_df
from helper import pd, logging
from helper.data_helper import check_missing_data, round_to_nearest_hour
from service.school_holidays_service import add_school_holidays


def merge_tables(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """

    datastore.training_data = merge_with_counts(datastore)
    print datastore.training_data.shape
    print datastore.training_data.head()
    datastore.training_data = merge_with_weather_day(datastore)
    print datastore.training_data.shape
    datastore.training_data = merge_with_public_holidays(datastore)
    print datastore.training_data.shape
    datastore.training_data = merge_with_regions(datastore)
    print datastore.training_data.shape
    datastore.training_data = merge_with_school_holidays(datastore)
    print datastore.training_data.shape
    return datastore.training_data


def merge_with_counts(datastore):
    """Summary

    Args:
        datastore (TYPE): Data_store

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


def merge_with_weather_day(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    df_weather_day = datastore.db.weather_day

    df_weather_day = df_weather_day[['maxtemperature', 'mintemperature',
                                     'weathersituation',
                                     'cloudamount',
                                     'day',
                                     'latitude', 'longitude']]
    df_weather_day.rename(
        columns={'day': 'date'}, inplace=True)

    datastore = match_coordinates(datastore, df_weather_day)
    datastore.training_data = datastore.training_data[
        ['idbldsite',
         'latitude',
         'longitude',
         'latitude_closest',
         'longitude_closest',
         'compensatedin',
         'date', 'date_time']]

    df_sites_counts_weather_day = pd.merge(datastore.training_data,
                                           df_weather_day,
                                           left_on=['latitude_closest',
                                                    'longitude_closest',
                                                    'date'],
                                           right_on=['latitude',
                                                     'longitude', 'date'],
                                           how='left',
                                           suffixes=['_sites', '_weather'],
                                           indicator=True)

    df_sites_counts_weather_day = check_missing_data(df_sites_counts_weather_day,
                                                     "left_only",
                                                     inspect.currentframe().f_code.co_name)

    df_sites_counts_weather_day = df_sites_counts_weather_day.drop(
        ['latitude_closest', 'longitude_closest', 'latitude_weather', 'longitude_weather'], 1)

    df_sites_counts_weather_day.rename(columns={'latitude_sites': 'latitude',
                                                'longitude_sites': 'longitude'}, inplace=True)

    print df_sites_counts_weather_day.columns
    return df_sites_counts_weather_day


def merge_with_public_holidays(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    df_holidays = datastore.db.public_holidays
    df_holidays = df_holidays[['idbldsite', 'day']]

    df_with_public_holidays = pd.merge(datastore.training_data, df_holidays,
                                       left_on=[
                                           'idbldsite', 'date'],
                                       right_on=['idbldsite', 'day'],
                                       how='left',
                                       suffixes=['_counts', '_holidays'])
    df_with_public_holidays[
        'is_public_holiday'] = ~df_with_public_holidays.day.isnull() * 1
    df_with_public_holidays = df_with_public_holidays.drop('day', 1)
    return df_with_public_holidays


def merge_with_regions(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    df_regions = create_regions_df(datastore)
    print df_regions
    df_with_regions = pd.merge(
        datastore.training_data, df_regions, on='idbldsite', how='left')
    return df_with_regions


def merge_with_school_holidays(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    return add_school_holidays(datastore.training_data)
