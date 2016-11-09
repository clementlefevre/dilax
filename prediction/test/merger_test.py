import pandas as pd
from mock import Mock
import pytest

from helper.data_merger import *
from service.geocoding_API_service import create_regions_df


@pytest.fixture(autouse=True, scope="module")
def datastore():
    datastore = Mock()

    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

    datastore.period = 'D'

    datastore.db.sites = pd.read_csv(
        'test/data/sites_sample3.csv', sep=';')

    datastore.db.counts = pd.read_csv(
        'test/data/counts_sample3.csv', sep=';', parse_dates=['timestamp'])

    datastore.db.weather_day = pd.read_csv(
        'test/data/weather_day_sample3.csv', sep=';', parse_dates=['day'])

    datastore.db.weather_intraday = pd.read_csv(
        'test/data/weather_intraday_sample3.csv', sep=';',
        parse_dates=['timestamp'], date_parser=dateparse)

    datastore.db.public_holidays = pd.read_csv(
        'test/data/public_holidays_sample3.csv', sep=";",
        parse_dates=['day'])
    return datastore


def test_merge(datastore, caplog):
    datastore.training_data = merge_with_counts(datastore)
    datastore.training_data = merge_with_weather(datastore)
    print "after merge_with_weather", datastore.training_data.head()
    datastore.training_data.to_csv('data/test_merge_region_hour.csv')
    datastore.training_data = merge_with_public_holidays(datastore)
    datastore.training_data.to_csv('data/test_merge_public_holidays_hour.csv')
    datastore.training_data = merge_with_regions(datastore)
    datastore.training_data = merge_with_school_holidays(datastore)
    print "after merge :", datastore.training_data.head()
    datastore.training_data.to_csv('data/test_merge_hour.csv')
    assert datastore.training_data.shape[0] > 0


def test_merge_with_regions_no_data(datastore, caplog):
    datastore.db.sites = pd.read_csv(
        'test/data/sites_sample2.csv', sep=';')

    df_regions = create_regions_df(datastore)

    assert df_regions.idbldsite.unique().tolist(
    ) == datastore.db.sites.idbldsite.unique().tolist()
