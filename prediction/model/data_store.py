import pandas as pd
from db_manager import DB_manager
from config_manager import Config_manager

from helper.data_merger import merge_tables
from helper.data_helper import add_calendar_fields, regularize


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
        self._training_set_(predictor.create)

    def _training_set_(self, create):
        training_data_file = self.config.data_store_settings['path'] + '/' + self.config.DB['db_name'] + \
            '_training_set_' + \
            self.period + '.csv'

        sites_infos_file = self.config.data_store_settings[
            'path'] + '/' + self.config.DB['db_name'] + 'sites_infos' + '.csv'

        if create:
            print "prepare new training set..."
            self.db = DB_manager(self.db_params)
            self.training_data = merge_tables(self)
            self.training_data = add_calendar_fields(self.training_data)
            self.training_data = regularize(self)
            self.training_data.to_csv(training_data_file, encoding='utf-8')
            self.sites_infos.to_csv(sites_infos_file, encoding='utf-8')
            print "finished preparing training set"
        else:
            try:
                self.training_data = pd.read_csv(training_data_file)

            except IOError as e:
                print "error by reading the csv file : {0}".format(training_data_file)
                print e.args

            try:
                self.sites_infos = pd.read_csv(sites_infos_file)
            except IOError as e:
                print "error by reading the csv file : {0}".format(sites_infos_file)
                print e.args
             print "finished reading training set"
