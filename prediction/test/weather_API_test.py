from service.weather_API_service import get_weather_forecasts
from model.config_manager import Config_manager


config_manager = Config_manager()


def test_get_weather_parameters():
    coordinates = (48.776630, 9.176330)
    weather_parameters = set(['ne', 'tn', 'tx', 'ww'])

    df_weather = get_weather_forecasts(
        config_manager.weather_API, *coordinates)
    assert df_weather['ww'].iloc[0] > 0
    df_weather_columns = set(df_weather.columns.tolist())

    assert weather_parameters.issubset(df_weather_columns)
