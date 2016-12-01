import app.business_logic.model.merger.abstract as abstract
from app.business_logic.service.weatherstore_service import get_weatherstore_forecasts


class WeatherForecastsDayMerger(abstract.Merger):

    def __init__(self):
        super(WeatherForecastsDayMerger, self).__init__(name="weather_forecasts_day",
                                                        left_on=[
                                                            'idbldsite', 'date'],
                                                        right_on=[
                                                            'idbldsite', 'date'],
                                                        suffixes=['_sites', ''], how='left', drop_missing=False)

        self.filter_columns = None
        self.drop_columns = ['id', 'period',
                             'data_type', 'updated', 'tt', 'site_id']
        self.rename_columns = {'tn': 'mintemperature',
                               'tx': 'maxtemperature',
                               'ww': 'weathersituation',
                               'ne': 'cloudamount',
                               'rrr': "precipitation_mm",
                               'prrr': 'precipitation_probability'}

    def _set_right_data(self):
        self.right = get_weatherstore_forecasts(self.datastore, self.left)
        self.right = self.right.rename(columns={'dateTime': 'date'})

    def _custom(self):
        self.merged = self.merged.fillna(0)


class WeatherForecastsHourMerger(abstract.Merger):

    def __init__(self):
        super(WeatherForecastsHourMerger, self).__init__(name="weather_forecasts_hour",
                                                         left_on=[
                                                             'idbldsite', 'date'],
                                                         right_on=[
                                                             'idbldsite', 'date'],
                                                         suffixes=['_sites', ''], how='left', drop_missing=False)

        self.filter_columns = None

        self.drop_columns = ['id', 'period',
                             'data_type', 'updated', 'tt', 'site_id']

        self.rename_columns = {'tn': 'mintemperature',
                               'tx': 'maxtemperature',
                               'ww': 'weathersituation',
                               'ne': 'cloudamount',
                               'rrr': "precipitation_mm",
                               'prrr': 'precipitation_probability'}

    def _set_right_data(self):
        self.right = get_weatherstore_forecasts(self.datastore, self.left)
        self.right = self.right.rename(columns={'dateTime': 'date'})

    def _custom(self):
        self.merged = self.merged.fillna(0)
