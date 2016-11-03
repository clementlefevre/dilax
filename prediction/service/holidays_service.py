
# coding: utf-8


import pandas as pd
import re
from datetime import date

dateparse = lambda x: pd.datetime.strptime(x, '%d.%m.%Y')
holidays = pd.read_csv("data/school_holidays/holidays_germany_2013_2016.csv",
                       date_parser=dateparse)
region = pd.read_csv('data/region_germany.csv')

holidays.head()


def create_holidays_df():
    pattern = re.compile("\.(?!\d)")
    holidays['from'] = holidays.Von.replace(pattern, "").str.slice(0, 5)
    holidays['to'] = holidays.Bis.replace(pattern, "").str.slice(0, 5)
    holidays.year = holidays.year.map(str)
    holidays['from_date'] = holidays[['from', 'year']].apply(
        lambda x: '.'.join(x), axis=1)
    holidays['to_date'] = holidays[['to', 'year']].apply(
        lambda x: '.'.join(x), axis=1)
    holidays['from_date_format'] = pd.to_datetime(holidays.from_date)
    holidays['to_date_format'] = pd.to_datetime(holidays.to_date)
    df = add_region_id()
    df.to_csv('data/school_holidays/holidays_germany_df.csv', index=0)


def add_region_id():
    df = pd.merge(holidays[['from_date', 'to_date', 'Bundesland']],
                  region, left_on='Bundesland', right_on='region_name')
    return df[['region_id', 'from_date', 'to_date']]


def reindex_holidays():
    df_school = pd.read_csv('data/school_holidays/holidays_germany_df.csv',
                            parse_dates=['from_date', 'to_date'], date_parser=dateparse)
    date_range = pd.date_range(date(2013, 1, 1), date(2017, 1, 10))
    df_school['from_date2'] = df_school.from_date
    df = df_school.groupby(['region_id']).apply(lambda x: x.set_index(
        'from_date').sort_index().reindex(date_range, method='ffill'))
    df = df.fillna(0)
    df = df.reset_index(level=1)
    df = df.rename(columns={'level_1': 'date', 'from_date2': 'from_date'})
    df['is_holiday'] = (df.date >= df.from_date) & (df.date <= df.to_date)
    df = df[['date', 'is_holiday']]

    df.to_csv('data/school_holidays/germany.csv')


def add_school_holidays(df):

    df_school_holidays = pd.read_csv(
        'data/school_holidays/germany.csv', parse_dates=['date'])

    df_with_school_holidays = pd.merge(df, df_school_holidays, on=[
                                       'region_id', 'date'], how='left', suffixes=['_data', '_school'])
    print df_with_school_holidays.head()
    return df_with_school_holidays
