
# coding: utf-8
import pandas as pd
from datetime import date


def dateparse(x):
    return pd.datetime.strptime(x, '%d.%m.%Y')


holidays = pd.read_csv("data/school_holidays.csv",
                       date_parser=dateparse)

region = pd.read_csv('data/regions_countries.csv')


def add_new_country_schedule(filename):
    df_holidays = reindex_holidays(filename)
    df_holidays = add_region_id(df_holidays)
    df_current_school_holidays = pd.read_csv(
        'data/school_holidays.csv', parse_dates=['date'])
    df_current_school_holidays = pd.concate(
        [df_holidays, df_current_school_holidays], axis=0)
    df_current_school_holidays.to_csv('data/school_holidays.csv')


def add_region_id():
    df = pd.merge(holidays[['from_date', 'to_date', 'region_name']],
                  region, on='region_name')
    return df[['region_id', 'from_date', 'to_date']]


def reindex_holidays(filename):
    df_school = pd.read_csv('data/school_holidays/' + filename + '.csv',
                            parse_dates=['from_date', 'to_date'],
                            date_parser=dateparse)
    date_range = pd.date_range(date(2013, 1, 1), date(2017, 1, 10))
    df_school['from_date2'] = df_school.from_date
    df = df_school.groupby(['region_id']).apply(lambda x: x.set_index(
        'from_date').sort_index().reindex(date_range, method='ffill'))
    df = df.fillna(0)
    df = df.reset_index(level=1)
    df = df.rename(columns={'level_1': 'date', 'from_date2': 'from_date'})
    df['is_holiday'] = (df.date >= df.from_date) & (df.date <= df.to_date)
    df = df[['date', 'is_holiday']]

    return df


def add_school_holidays(df):

    df_school_holidays = pd.read_csv(
        'data/school_holidays.csv', parse_dates=['date'])

    df_with_school_holidays = pd.merge(df, df_school_holidays,
                                       on=['region_id', 'date'],
                                       how='left',
                                       suffixes=['_data', '_school'])
    return df_with_school_holidays
