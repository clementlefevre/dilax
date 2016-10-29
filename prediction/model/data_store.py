from db_manager import DB_manager
from model import pd
from helper.data_helper import *
from helper.data_merger import *


class Data_store(object):
    def __init__(self, name, db_params={}):
        self.name = name
        self.db = DB_manager(**db_params)
        self.__create_data()

    def __create_data(self):
        print "creating data..."
        self.data_day = merge_with_weather_day(self)
        self.data_day = merge_with_counts(self)
        self.data_day = merge_with_public_holidays(self)
        # self.data_day = merge_with_school_holidays(self)

        # df['day_of_week'] = df.date.dt.dayofweek
        # df['day_of_month'] = df.date.dt.day
        # df['month'] = df.date.dt.month
        # df['year'] = df.date.dt.year

        # for day in range(0, 7):
        #     df['day_' + str(day)] = np.where(df.day_of_week == day, 1, 0)

        # for month in range(1, 13):
        #     df['month_' + str(month)] = np.where(df.month == month, 1, 0)

        # df['holiday'] = np.where(df.is_holiday != 0, 1, 0)
        # df['not_holiday'] = np.where(df.is_holiday == 0, 1, 0)

        # # Filter on counts with weather data
        # df = df[~df.maxtemperature.isnull()]
        # df.to_csv("data/counts_sites_weather_holidays_day.csv", index_col=True)
        print "finished creating data"
