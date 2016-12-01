import app.business_logic.model.merger.abstract as abstract
from app.business_logic.helper.data_helper import match_coordinates


def add_idbldsite_to_weather_data(df, datastore):
    weather_coordinates = df[['latitude', 'longitude']].drop_duplicates()
    df_sites = datastore.db_manager.sites
    weather_coordinates_matched = match_coordinates(
        weather_coordinates, df_sites)

    weather_coordinates_matched_sites = abstract.pd.merge(weather_coordinates_matched,
                                                          df_sites,
                                                          left_on=[
                                                              'latitude_closest', 'longitude_closest'],
                                                          right_on=['latitude', 'longitude'])

    weather_coordinates_matched_sites = weather_coordinates_matched_sites[
        ['idbldsite', 'latitude_x', 'longitude_x']]
    df = abstract.pd.merge(df, weather_coordinates_matched_sites, left_on=['latitude', 'longitude'],
                           right_on=['latitude_x', 'longitude_x'])

    return df


class WeatherObservationsDayMerger(abstract.Merger):

    def __init__(self):
        super(WeatherObservationsDayMerger, self).__init__(name="weather_observations_day",
                                                           left_on=[
                                                               'idbldsite', 'date'],
                                                           right_on=[
                                                               'idbldsite', 'date'],
                                                           suffixes=['_sites', ''])

        self.filter_columns = ['idbldsite', 'compensatedin',
                               'date', 'date_time',
                               'maxtemperature', 'mintemperature',
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
        df = add_idbldsite_to_weather_data(df, self.datastore)
        self.right = df


class WeatherObservationsHourMerger(abstract.Merger):

    def __init__(self):
        super(WeatherObservationsHourMerger, self).__init__(name="weather_observations_hour",
                                                            left_on=['idbldsite',
                                                                     'date_',
                                                                     'hour'],
                                                            right_on=['idbldsite',
                                                                      'date_', 'hour'],
                                                            how='left',
                                                            suffixes=[
                                                                '_sites', '_weather'],
                                                            indicator=True)

        self.filter_columns = None
        self.rename_columns = {'latitude_sites': 'latitude',
                               'longitude_sites': 'longitude'}

    def _set_right_data(self):

        df_weather_hour = self._combine_with_day_weather()

        df_weather_hour = self._reindex_weather_intraday(df_weather_hour)

        df_weather_hour = df_weather_hour[~df_weather_hour.latitude.isnull()]

        df_weather_hour = add_idbldsite_to_weather_data(
            df_weather_hour, self.datastore)

        df_weather_hour['hour'] = df_weather_hour['timestamp'].dt.hour
        df_weather_hour['date_'] = df_weather_hour['timestamp'].dt.date

        self.right = df_weather_hour

    def _set_left_data(self, data):
        data['hour'] = data['date_time'].dt.hour
        data['date_'] = data['date_time'].dt.date
        self.left = data

    def _custom(self):
        self.merged[
            'mintemperature'] = self.merged.temperature
        self.merged[
            'maxtemperature'] = self.merged.temperature

    def _combine_with_day_weather(self):
        df_weather_hour = self.datastore.db_manager.weather_intraday

        df_weather_day = self.datastore.db_manager.weather_day

        df_weather_hour = abstract.pd.merge(df_weather_hour,
                                            df_weather_day[
                                                ['id', 'latitude', 'longitude']],
                                            left_on='idparent',
                                            right_on='id')

        df_weather_hour = df_weather_hour[['timestamp', 'temperature',
                                           'cloudamount', 'weathersituation',
                                           'latitude', 'longitude']]

        return df_weather_hour

    def _reindex_weather_intraday(self, df_weather_hour):
        date_range = abstract.pd.date_range(df_weather_hour.timestamp.min(
        ), df_weather_hour.timestamp.max(), freq='H')

        df_weather_hour['coord'] = df_weather_hour.latitude.map(
            str) + ";" + df_weather_hour.longitude.map(str)

        date_range = abstract.pd.date_range(df_weather_hour.timestamp.min(
        ), df_weather_hour.timestamp.max(), freq='H')

        df_weather_hour_clean = df_weather_hour.drop_duplicates()
        df_weather_hour_clean.shape

        groupy = df_weather_hour_clean.groupby(['coord'])
        df = abstract.pd.DataFrame()
        for name, group in groupy:
            group = group.set_index('timestamp').sort_index()
            group = group[~group.index.duplicated()]
            group = group.reindex(date_range, method='ffill')
            df = abstract.pd.concat([df, group], axis=0)

        df = df.reset_index()
        df = df.rename(columns={"index": "timestamp"})
        return df
