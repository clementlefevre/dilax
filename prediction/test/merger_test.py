import pandas as pd
from mock import Mock
import pytest

from helper.data_merger import merge_with_counts, merge_with_weather_day, merge_with_regions
from service.geocoding_API_service import create_regions_df


@pytest.fixture(autouse=True, scope="module")
def datastore():
    datastore = Mock()

    datastore.db.sites = pd.read_csv(
        'test/data/sites_sample1.csv', sep=';')

    datastore.db.counts = pd.read_csv(
        'test/data/counts_sample1.csv', sep=';', parse_dates=['timestamp'])

    datastore.db.weather_day = pd.read_csv(
        'test/data/weather_day_sample1.csv', sep=';', parse_dates=['day'])

    return datastore


def test_merge_with_counts(datastore, caplog):
    df_merged = merge_with_counts(datastore)
    log = caplog.text()
    assert 'No data found for' in log


def test_merge_with_weather_day(datastore, caplog):
    datastore.training_data = merge_with_counts(datastore)
    df_merged = merge_with_weather_day(datastore)

    log = caplog.text()
    assert 'After merge_with_weather_day, No data found for:' in log


def test_merge_with_regions(datastore, caplog):
    datastore.training_data = merge_with_counts(datastore)
    datastore.training_data = merge_with_weather_day(datastore)

    datastore.training_data = merge_with_regions(datastore)
    print datastore.training_data
    assert datastore.training_data.shape[0] > 0
