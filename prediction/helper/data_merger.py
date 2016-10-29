from data_helper import round_coordinates
from service.geocoding_service import create_regions_df
from helper import pd


def merge_with_weather_day(datastore):
    df_weather_day = round_coordinates(datastore.db.weather_day)
    df_sites = round_coordinates(datastore.db.sites)
    df_weather_day = df_weather_day[['maxtemperature', 'mintemperature',
                                     'weathersituation',
                                     'cloudamount', 'day', 'coord']]
    df_sites = df_sites[['idbldsite', 'coord', 'sname']]

    df_sites_weather_day = pd.merge(df_weather_day, df_sites,
                                    on='coord',
                                    how='left', suffixes=['_weather', '_sites'])

    df_sites_weather_day.rename(columns={'day': 'date'}, inplace=True)
    return df_sites_weather_day


def merge_with_counts(datastore):
    df_counts = datastore.db.counts

    df_counts['date'] = pd.to_datetime(df_counts.timestamp.dt.date)
    df_counts = df_counts.groupby(['idbldsite', 'date']).sum()
    df_counts = df_counts.reset_index()
    df_counts = df_counts[['idbldsite', 'compensatedin', 'date']]

    df_counts_sites_weather_day = pd.merge(df_counts, datastore.data_day, how='left', left_on=[
        'idbldsite', 'date'], right_on=['idbldsite', 'date'], suffixes=['counts_', 'weather_'])

    return df_counts_sites_weather_day


def merge_with_public_holidays(datastore):
    df_holidays = datastore.db.public_holidays
    df_holidays = df_holidays[['idbldsite', 'day']]

    df_with_public_holidays = pd.merge(datastore.data_day, df_holidays, left_on=[
        'idbldsite', 'date'], right_on=['idbldsite', 'day'], how='left', suffixes=['_counts', '_holidays'])
    df_with_public_holidays[
        'is_public_holiday'] = ~df_with_public_holidays.day.isnull() * 1
    df_with_public_holidays = df_with_public_holidays.drop('day', 1)
    return df_with_public_holidays


def merge_with_regions(datastore):
    df_regions = create_regions_df(datastore)
    df_with_regions = pd.merge(
        datastore.data_day, df_regions, on='idbldsite')
    return df_with_regions


def merge_with_school_holidays(datastore):
    df_school_holidays = pd.read_csv(
        'data/school_holidays/germany.csv', parse_dates=['date'])

    df_with_school_holidays = pd.merge(
        datastore.data_day, df_school_holidays, on=['region_id', 'date'], how='left', suffixes=['_data', '_school'])
    df_with_school_holidays = df_with_school_holidays.drop('region', 1)
    return df_with_school_holidays
