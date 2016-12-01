from datetime import datetime
import app.business_logic.model.merger.abstract as abstract


class DatesMerger(abstract.Merger):

    def __init__(self):
        super(DatesMerger, self).__init__(name="dates",
                                          left_on=['idbldsite'],
                                          right_on=['idbldsite'],
                                          suffixes=['', ''])
        self.filter_columns = None

    def _set_right_data(self):
        date_ranges = abstract.pd.date_range(
            self.datastore.predict_from, self.datastore.predict_to,
            freq=self.datastore.period)

        df_dates = abstract.pd.DataFrame(date_ranges, columns=['date'])

        df_dates['date_time'] = df_dates.date

        if self.datastore.period == 'H':
            self._add_date_column(df_dates)

        df_dates = self._add_site_id(df_dates)
        self.right = df_dates

    def _set_left_data(self, data):
        self.left = data[['idbldsite', 'region_id']].drop_duplicates()

    def _add_date_column(self, df):
        df['date_time'].apply(lambda dt:
                              datetime(dt.year,
                                       dt.month,
                                       dt.day))

    def _add_site_id(self, df):
        df_all_sites = abstract.pd.DataFrame()
        sites_id = self.left['idbldsite'].tolist()

        for site_id in sites_id:
            df['idbldsite'] = site_id
            df_all_sites = abstract.pd.concat([df_all_sites, df], axis=0)

        return df_all_sites
