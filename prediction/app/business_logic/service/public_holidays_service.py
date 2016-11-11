import pandas as pd


def add_public_holidays(df_data, df_holidays):

    df_holidays.day = pd.to_datetime(df_holidays.day)
    df_holidays = df_holidays[['idbldsite', 'day']]

    merged = pd.merge(df_data, df_holidays,
                      left_on=['idbldsite',
                               'date'],
                      right_on=['idbldsite', 'day'],
                      how='left',
                      suffixes=['_counts', '_holidays'])
    merged[
        'is_public_holiday'] = ~merged.day.isnull() * 1
    merged = merged.drop('day', 1)

    return merged
