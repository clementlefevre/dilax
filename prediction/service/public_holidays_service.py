import pandas as pd


def add_public_holidays(df, datastore):
    df_holidays = datastore.db.public_holidays
    df_holidays = df_holidays[['idbldsite', 'day']]

    df_with_public_holidays = pd.merge(df, df_holidays,
                                       left_on=['idbldsite',
                                                'date'],
                                       right_on=['idbldsite', 'day'],
                                       how='left',
                                       suffixes=['_counts', '_holidays'])
    df_with_public_holidays[
        'is_public_holiday'] = ~df_with_public_holidays.day.isnull() * 1
    df_with_public_holidays = df_with_public_holidays.drop('day', 1)
    return df_with_public_holidays
