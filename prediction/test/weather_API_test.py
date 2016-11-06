import pytest
from model.data_store import Data_store
from model.predictor import Predictor
from service.weather_API_service import get_weather_forecasts
from model.config_manager import Config_manager


config_manager = Config_manager()


@pytest.fixture(autouse=True, scope="module")
def store():
    return Data_store(Predictor("DWE_CLOSED_2013", create=False))


def test_get_weather_parameters(store):
    coordinates = (48.776630, 9.176330)
    weather_parameters = set(['ne', 'tn', 'tx', 'ww'])

    df_weather = get_weather_forecasts(
        config_manager.weather_API, *coordinates)
    assert df_weather['ww'].iloc[0] > 0
    df_weather_columns = set(df_weather.columns.tolist())

    assert weather_parameters.issubset(df_weather_columns)
