from datetime import datetime

import app.business_logic.model.merger.abstract as abstract


class ConversionDayMerger(abstract.Merger):

    def __init__(self):
        super(ConversionDayMerger, self).__init__(name="conversion_day",
                                                  left_keys=[
                                                      'idbldsite', 'date'],
                                                  right_keys=[
                                                      'idbldsite', 'date'],
                                                  how='left', suffixes=['_counts', '_conversion'],
                                                  drop_missing=False)
        self.filter_columns = None

        self.drop_columns = ['id', 'timefrom', 'timeto']

    def _set_right_data(self):
        df_conversion_day = self.datastore.db_manager.conversion
        df_conversion_day['date'] = df_conversion_day['timefrom'].apply(
            lambda dt: datetime(dt.year, dt.month, dt.day))
        df_conversion_day = df_conversion_day.groupby(
            ['idbldsite', 'date']).sum()
        df_conversion_day = df_conversion_day.reset_index()
        self.right = df_conversion_day
