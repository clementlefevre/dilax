import configparser


class Config_manager(object):
    def __init__(self):
        self.__read_config()

    def __read_config(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.DB = config._sections['db_settings']
        self.features = config._sections['features_settings']
        self.features = self.__convert_to_boolean()

    def __convert_to_boolean(self):
        features = {}
        for k, v in self.features.items():
            features[k] = bool(int(v))
        return features
