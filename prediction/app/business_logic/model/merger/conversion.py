from datetime import datetime

import app.business_logic.model.merger.abstract as abstract


class ConversionDayMerger(abstract.Merger):

    def __init__(self):
        super(ConversionDayMerger, self).__init__(name="conversion_day",
                                                  left_on=[
                                                      'idbldsite', 'date'],
                                                  right_on=[
                                                      'idbldsite', 'date'],
                                                  how='left', suffixes=['_counts', '_conversion'],
                                                  drop_missing=False)
        self.filter_columns = None

        self.drop_columns = ['id', 'timefrom', 'timeto']

    def _set_right_data(self):
        df_conversion_day = self.datastore.db_manager.conversion
        if df_conversion_day.empty:
            self.right = None
            return
        df_conversion_day['date'] = df_conversion_day['timefrom'].apply(
            lambda dt: datetime(dt.year, dt.month, dt.day))
        df_conversion_day = df_conversion_day.groupby(
            ['idbldsite', 'date']).sum()
        df_conversion_day = df_conversion_day.reset_index()
        self.right = df_conversion_day


class ConversionHourMerger(abstract.Merger):

    def __init__(self):
        super(ConversionHourMerger, self).__init__(name="conversion_hour",
                                                   left_on=[
                                                       'idbldsite', 'date_time'],
                                                   right_on=[
                                                       'idbldsite', 'timefrom'],
                                                   how='left', suffixes=['_counts', '_conversion'],
                                                   drop_missing=False)
        self.filter_columns = None

        self.drop_columns = ['id', 'timefrom', 'timeto']

    def _set_right_data(self):
        self.right = self.datastore.db_manager.conversion
