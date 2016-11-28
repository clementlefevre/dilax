import app.business_logic.model.merger.abstract as abstract
from app.business_logic.helper.data_helper import add_idbldsite_to_weather_data


class WeatherDayMerger(abstract.Merger):

    def __init__(self):
        super(WeatherDayMerger, self).__init__(name="weather_hour",
                                               left_keys=['idbldsite', 'date'], right_keys=['idbldsite', 'date'], suffixes=['_sites', ''])

        self.filter_columns = ['idbldsite', 'compensatedin',
                               'date', 'date_time', 'maxtemperature', 'mintemperature',
                               'weathersituation', 'cloudamount']

        self.rename_columns = {'latitude_sites': 'latitude',
                               'longitude_sites': 'longitude'}

    def _set_right_data(self):
        df = self.datastore.db_manager.weather_day
        df = df[['maxtemperature', 'mintemperature',
                 'weathersituation',
                 'cloudamount',
                 'day',
                 'latitude', 'longitude']]
        df.rename(
            columns={'day': 'date'}, inplace=True)
        df = add_idbldsite_to_weather_data(
            self.datastore, df)
        self.right = df
