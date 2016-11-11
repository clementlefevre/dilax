import pytest
from model.datastore import Datastore


@pytest.fixture(autouse=True, scope="function")
def store():

    db_params_1 = {'db_user': 'dwe-arcadia', 'db_name': 'DWE_ARCADIA_2015',
                   'db_port': '5432', 'db_pwd': 'VtJ5Cw3PKuOi4i3b',
                   'db_url': 'localhost'}

    db_params_2 = {'db_user': 'dwe-closed', 'db_name': 'DWE_CLOSED_2013',
                   'db_port': '5432', 'db_pwd': '6EVAqWxOsX2Ao', 'db_url': 'localhost'}

    datastore = Datastore(db_params_2, period='H',
                          create=True, dt_to='2017-01-15')
    datastore.get_data()
    return datastore


def test_datastore_hour(store):

    store.create_forecasts()

    assert store.training_data.shape[0] > 0
    assert store.forecasts.shape[0] > 0
