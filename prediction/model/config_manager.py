import configparser


class Config_manager(object):
    def __init__(self):
        self.__read_config()

    def __read_config(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.DB = config._sections['db_settings']
        self.data_store_settings = config._sections['data_store_settings']
        self.weather_API = config._sections['weather_API']
        self.features = config._sections['features_settings']

        self.features = self._convert_to_boolean()

    def _convert_to_boolean(self):
        features = {}
        for k, v in self.features.items():
            features[k] = bool(int(v))
        return features
