import pandas as pd
from datetime import datetime


def merge_with_conversion(datastore):
    if datastore.period == 'H':
        return add_conversion_hour(datastore)

    if datastore.period == 'D':
        return add_conversion_day(datastore)


def add_conversion_hour(datastore):
    merged = pd.merge(datastore.training_data, datastore.db.conversion, left_on=['idbldsite', 'date_time'],
                      right_on=['idbldsite', 'timefrom'], how='left')

    merged = merged.drop(['id', 'timefrom', 'timeto'], 1)
    return merged


def add_conversion_day(datastore):
    df_conversion_day = datastore.db.conversion
    df_conversion_day['date'] = df_conversion_day['timefrom'].apply(
        lambda dt: datetime(dt.year, dt.month, dt.day))
    df_conversion_day = df_conversion_day.groupby(
        ['idbldsite', 'date']).sum()
    df_conversion_day = df_conversion_day.reset_index()

    merged = pd.merge(datastore.training_data, df_conversion_day, left_on=['idbldsite', 'date'],
                      right_on=['idbldsite', 'date'], how='left')
    merged = merged.drop('id', 1)
    merged.to_csv("data/conversion_day.csv")
    return merged
