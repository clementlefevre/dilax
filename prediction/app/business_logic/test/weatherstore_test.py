import pytest
from mock import Mock, MagicMock
import datetime

from ..service.weatherstore_service import get_weather_forecasts


@pytest.fixture(autouse=True, scope="function")
def datastore():
    datastore = Mock()
    datastore.date_from = datetime.date(2016, 11, 18)
    datastore.period = "D"

    datastore.db_params = {}
    datastore.db_params['db_user'] = "dwe-closed"
    return datastore


@pytest.fixture(autouse=True, scope="function")
def df():
    df = Mock()
    df.idbldsite = Mock()
    df.idbldsite.unique = MagicMock(return_value=[1])
    return df


def test_weatherstore_connection(datastore, df):

    df_weatherstore = get_weather_forecasts(datastore, df)
    print df_weatherstore.head()
    assert df_weatherstore.shape[0] > 0
