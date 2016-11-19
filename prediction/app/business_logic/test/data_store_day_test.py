from datetime import datetime, timedelta
from ..model.datastore import Datastore

import pandas as pd
import pytest


@pytest.fixture(autouse=True, scope="function")
def store():
    db_params_1 = {'db_user': 'dwe-arcadia', 'db_name': 'DWE_ARCADIA_2015',
                   'db_port': '5432', 'db_pwd': 'VtJ5Cw3PKuOi4i3b',
                   'db_url': 'localhost'}

    db_params_2 = {'db_user': 'dwe-closed', 'db_name': 'DWE_CLOSED_2013',
                   'db_port': '5432', 'db_pwd': '6EVAqWxOsX2Ao', 'db_url': 'localhost'}

    datastore = Datastore(db_params_2, create=True, dt_to='2017-01-15')
    datastore.get_data()
    return datastore


def test_create_datastore(store):
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
