"""Summary
"""
from data_helper import get_nearest_coordinate
from service.geocoding_API_service import create_regions_df
from helper import pd
from service.school_holidays_service import add_school_holidays


def merge_tables(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    if datastore.period == 'D':
        datastore.training_data = merge_with_weather_day(datastore)
        datastore.training_data = merge_with_counts(datastore)
        datastore.training_data = merge_with_public_holidays(datastore)
        datastore.training_data = merge_with_regions(datastore)
        datastore.training_data = merge_with_school_holidays(datastore)
        return datastore.training_data
    else:
        print "merge_tables not implemented for intraday"


def merge_with_weather_day(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    df_weather_day = datastore.db.weather_day
    df_sites = datastore.db.sites
    df_sites['latitude_closest'] = df_sites.latitude.apply(
        lambda x: get_nearest_coordinate(x, df_weather_day.latitude))

    df_sites['longitude_closest'] = df_sites.longitude.apply(
        lambda x: get_nearest_coordinate(x, df_weather_day.longitude))

    df_weather_day = df_weather_day[['maxtemperature', 'mintemperature',
                                     'weathersituation',
                                     'cloudamount', 'day', 'latitude', 'longitude']]
    df_sites = df_sites[
        ['idbldsite', 'latitude_closest', 'longitude_closest', 'sname']]

    df_sites_weather_day = pd.merge(df_weather_day, df_sites,
                                    left_on=['latitude', 'longitude'],
                                    right_on=['latitude_closest',
                                              'longitude_closest'],
                                    how='left',
                                    suffixes=['_weather', '_sites'], indicator=True)

    df_sites_weather_day.rename(columns={'day': 'date'}, inplace=True)
    df_sites_weather_day = df_sites_weather_day.drop(
        ['latitude_closest', 'longitude_closest'], 1)
    return df_sites_weather_day


def merge_with_counts(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    df_counts = datastore.db.counts

    df_counts['date'] = pd.to_datetime(df_counts.timestamp.dt.date)
    df_counts = df_counts.groupby(['idbldsite', 'date']).sum()
    df_counts = df_counts.reset_index()
    df_counts = df_counts[['idbldsite', 'compensatedin', 'date']]

    df_counts_sites_weather_day = pd.merge(df_counts,
                                           datastore.training_data,
                                           how='left',
                                           left_on=['idbldsite', 'date'],
                                           right_on=['idbldsite', 'date'],
                                           suffixes=['counts_', 'weather_'])

    return df_counts_sites_weather_day


def merge_with_public_holidays(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    df_holidays = datastore.db.public_holidays
    df_holidays = df_holidays[['idbldsite', 'day']]

    df_with_public_holidays = pd.merge(datastore.training_data, df_holidays, left_on=[
        'idbldsite', 'date'], right_on=['idbldsite', 'day'], how='left', suffixes=['_counts', '_holidays'])
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

    df_with_regions = pd.merge(
        datastore.training_data, df_regions, on='idbldsite')
    return df_with_regions


def merge_with_school_holidays(datastore):
    """Summary

    Args:
        datastore (TYPE): Description

    Returns:
        TYPE: Description
    """
    return add_school_holidays(datastore.training_data)
