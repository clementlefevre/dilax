import pytest
from model.data_store import Data_store


@pytest.fixture(autouse=True, scope="module")
def store():

    db_params = {'db_user': 'dwe-closed', 'db_name': 'DWE_CLOSED_2013',
                 'db_port': '5432', 'db_pwd': '6EVAqWxOsX2Ao', 'db_url': 'localhost'}

    data_store = Data_store(db_params, period='H',
                            create=True, date_to='2017-01-15')
    data_store.get_data()
    return data_store


def test_datastore_hour(store):

    store.create_forecasts()

    assert store.training_data.shape[0] > 0
    assert store.forecasts.shape[0] > 0
