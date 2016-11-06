import pandas as pd
import urllib2
import urllib
import json
from pandas.io.json import json_normalize
from model.config_manager import Config_manager


config_manager = Config_manager()


def add_weather_forecasts(datastore, df):
    site_id = df.idbldsite.unique()[0]
    site_infos = (datastore.sites_infos_dict[site_id])
    coordinates = (site_infos['latitude'], site_infos['longitude'])
    df_weather = get_weather_forecasts(
        config_manager.weather_API, *coordinates)
    df = pd.merge(
        df, df_weather[['date', 'ne', 'tn', 'tx', 'ww']],
        on='date', how='left')

    df.rename(columns={'tn': 'mintemperature',
                       'tx': 'maxtemperature',
                       'ww': 'weathersituation',
                       'ne': 'cloudamount'},
              inplace=True)

    return df


def get_weather_forecasts(config_weather, latitude, longitude):
    url_params = dict([('customer', config_weather['customer']),
                       ('hash', config_weather['hash']),
                       ('locsearchtype',
                        config_weather['locsearchtype']),
                       ('lat', latitude), ('lon', longitude)])

    try:
        response = urllib2.urlopen(
            config_weather['url_base'] + urllib.urlencode(url_params))
        data = json.loads(response.read())
        df_weather = json_normalize(
            data['LocationWeather']['forecast']['days'])
        df_weather.date = pd.to_datetime(df_weather.date)

        last_update = json_normalize(data['LocationWeather']['latestobservation'][
            'latests'][0])['dateTime'].values[0]

        df_weather['lastUpdate'] = pd.to_datetime(last_update)

        return df_weather
    except Exception as e:
        print e.message
        print "Could not retrieve weather data for :{};{}".format(latitude, longitude)
