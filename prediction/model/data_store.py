import pandas as pd
from db_manager import DB_manager
from config_manager import Config_manager

from helper.data_merger import merge_tables
from helper.data_helper import add_calendar_fields, regularize
from service.predictor_service import create_forecasts_data


class Data_store(object):
    def __init__(self, predictor, db_params={}):
        print 'create'
        print predictor
        self.db_name = predictor.db_name
        self.config = Config_manager()
        if not db_params:
            self.db_params = self.config.DB
        else:
            self.db_params = db_params

        self.period = predictor.period
        self.db = DB_manager(self.db_params)

        self._training_set_(predictor.create)

    def _training_set_(self, create):
        file_name = self.config.data_store_settings['path'] + '/training_set_' + \
            self.period + "_" + self.config.DB['db_name'] + '.csv'

        if create:
            print "prepare new training set..."
            self.data = merge_tables(self)
            self.data = add_calendar_fields(self.data)
            self.data = regularize(self)
            self.data.to_csv(file_name, encoding='utf-8')
            print "finished preparing training set"
        else:
            print file_name
            try:
                self.data = pd.read_csv(file_name)
            except IOError:
                print "error by reading the csv file {0}".format(file_name)
