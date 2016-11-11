import configparser
import logging
from collections import namedtuple


class Config_manager(object):
    def __init__(self):
        self.__read_config()

    def __read_config(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.DB = config._sections['db_settings']
        self.datastore_settings = config._sections['datastore_settings']
        self.weather_API = config._sections['weather_API']
        self.geocoding_API = config._sections['geocoding_API']
        self.features = config._sections['features_settings']

        self.features = self._convert_to_features()

    def _convert_to_features(self):
        Feature = namedtuple("Feature", ("regularize", "is_predictor"))
        features = {}
        for k, v in self.features.items():
            try:
                values = map(lambda x: bool(int(x)), tuple(v.split(",")))
                features[k] = Feature(*values)
            except Exception as e:
                logging.error(
                    "Could not read the configuration features :{}".format(e.args))
                raise
        return features
