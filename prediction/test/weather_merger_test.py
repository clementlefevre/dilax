import pandas as pd
from mock import MagicMock

from model.config_manager import Config_manager
from model.db_manager import DB_manager
from helper.data_helper import get_closest


def test_find_weather_infos_for_site_1():
    config_manager = Config_manager()
    db_manager = DB_manager(config_manager.DB)

    df_weather_day = db_manager.weather_day
    df_sites = db_manager.sites

    df_sites['latitude_closest'] = df_sites.latitude.apply(
        lambda x: get_closest(x,
                              df_weather_day.latitude))

    df_weather_day = df_weather_day[['maxtemperature', 'mintemperature',
                                     'weathersituation',
                                     'cloudamount', 'day', 'latitude']]
    df_sites = df_sites[['idbldsite', 'latitude_closest', 'sname']]

    df_sites_weather_day = pd.merge(df_weather_day, df_sites,
                                    left_on='latitude', right_on='latitude_closest',
                                    how='left', suffixes=['_weather', '_sites'])
    assert df_sites_weather_day[
        df_sites_weather_day.idbldsite == 1].maxtemperature.max() > 1
