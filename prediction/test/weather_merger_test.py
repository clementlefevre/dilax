import pandas as pd
from mock import Mock
import pytest
from helper.data_merger import merge_with_weather_day
from model.db_manager import DB_manager
from helper.data_helper import get_nearest_coordinate


@pytest.fixture(autouse=True, scope="module")
def datastore():
    db_params = {'db_user': 'dwe-arcadia', 'db_name': 'DWE_ARCADIA_2015',
                 'db_port': '5432', 'db_pwd': 'VtJ5Cw3PKuOi4i3b',
                 'db_url': 'localhost'}
    db_manager = DB_manager(db_params)
    datastore = Mock()
    datastore.db = db_manager
    return datastore


def test_log_if_missing_weather_data(datastore):
    print datastore.db.sites
    df = merge_with_weather_day(datastore)
    print df.head()
    assert df.shape[0] > 1


def test_find_weather_infos_for_site_1(datastore):

    df_weather_day = datastore.db.weather_day
    df_sites = datastore.db.sites

    df_sites['latitude_closest'] = df_sites.latitude.apply(
        lambda x: get_nearest_coordinate(x,
                                         df_weather_day.latitude))

    df_weather_day = df_weather_day[['maxtemperature', 'mintemperature',
                                     'weathersituation',
                                     'cloudamount', 'day', 'latitude']]
    df_sites = df_sites[['idbldsite', 'latitude_closest', 'sname']]

    df_sites_weather_day = pd.merge(df_weather_day, df_sites,
                                    left_on='latitude',
                                    right_on='latitude_closest',
                                    how='left',
                                    suffixes=['_weather', '_sites'])
    assert df_sites_weather_day[
        df_sites_weather_day.idbldsite == 1].maxtemperature.max() > 1
