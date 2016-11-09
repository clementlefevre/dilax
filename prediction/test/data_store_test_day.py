from datetime import datetime, timedelta
from model.data_store import Data_store

import pandas as pd
import pytest


@pytest.fixture(autouse=True, scope="module")
def store():
    db_params = {'db_user': 'dwe-arcadia', 'db_name': 'DWE_ARCADIA_2015',
                 'db_port': '5432', 'db_pwd': 'VtJ5Cw3PKuOi4i3b',
                 'db_url': 'localhost'}

    db_params = {'db_user': 'dwe-closed', 'db_name': 'DWE_CLOSED_2013',
                 'db_port': '5432', 'db_pwd': '6EVAqWxOsX2Ao', 'db_url': 'localhost'}

    data_store = Data_store(db_params, create=True, date_to='2017-01-15')
    data_store.get_data()
    return data_store


def test_create_data_store(store):
    assert store.db.engine is not None


def test_sites_query(store):
    sites = store.db.sites
    assert isinstance(sites, pd.DataFrame)
    assert sites.shape[0] > 0
    assert sites.shape[1] > 0


def test_data_content(store):
    assert store.training_data.shape[0] > 0


def test_geocoding(store):
    assert store.training_data['region_id'].shape[0] > 0


def test_predictor_period(store):
    assert store.period == "D"
    assert store.date_from == datetime.now().date()
    assert store.training_data.shape[0] > 0


def test_datastore_create_forecasts(store):
    store.create_forecasts()
    assert store.forecasts.shape[0] > 0
