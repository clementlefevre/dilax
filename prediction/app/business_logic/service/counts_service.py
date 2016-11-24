import os
import pandas as pd
from datetime import datetime
from ..helper.file_helper import get_file_path
from ..helper.data_helper import round_to_nearest_hour


def get_counts(datastore, date_from=None, date_to=None):
    print "date_from,date_to", date_from, date_to
    df_counts = datastore.db.counts

    df_counts['date'] = pd.to_datetime(df_counts.timestamp.dt.date)

    if date_from is not None:
        df_counts = df_counts[(df_counts.date >= date_from)]

    if date_to is not None:
        df_counts = df_counts[(df_counts.date < date_to)]

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

    return df_counts
