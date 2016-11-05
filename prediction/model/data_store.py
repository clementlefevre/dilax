import pandas as pd
from db_manager import DB_manager
from config_manager import Config_manager

from helper.data_merger import merge_tables
from helper.data_helper import add_calendar_fields, regularize
from service.predictor_service import create_forecasts_data


class Data_store(object):
    def __init__(self, predictor, db_params={}):
        self.db_name = predictor.db_name
        self.config = Config_manager()
        if not db_params:
            self.db_params = self.config.DB
        else:
            self.db_params = db_params
        self.period = predictor.period
        self.date_from = predictor.date_from
        self.date_to = predictor.date_to

        self._set_file_names()
        self._training_set_(predictor.create)
        self.create_sites_dict()

    def _training_set_(self, create):

        if create:
            print "prepare new training set..."
            self.db = DB_manager(self.db_params)
            self.training_data = merge_tables(self)
            self.training_data = add_calendar_fields(self.training_data)
            self.training_data = regularize(self)
            self.training_data.to_csv(
                self.file_names['training_set'], encoding='utf-8')
            self.sites_infos.to_csv(
                self.file_names['sites_info'], encoding='utf-8')
            print "finished preparing training set"
        else:
            try:
                self.training_data = pd.read_csv(
                    self.file_names['training_set'], parse_dates=['date'])

            except IOError as e:
                print "error by reading the csv file : {0}".\
                    format(self.file_names['training_set'])
                print e.args

            try:
                self.sites_infos = pd.read_csv(self.file_names['sites_info'])
            except IOError as e:
                print "error by reading the csv file : {0}".\
                    format(self.file_names['sites_info'])
                print e.args
            print "finished reading training set"

    def create_sites_dict(self):
        self.sites_infos_dict = self.sites_infos.set_index(
            'idbldsite').T.to_dict()

    def create_forecasts(self):
        self.forecasts = create_forecasts_data(self)
        self.forecasts.to_csv(
            self.file_names['forecasts_set'], encoding='utf-8')

    def _set_file_names(self):
        training_set = self.config.data_store_settings['path'] +\
            '/' + self.config.DB['db_name'] + \
            '_training_set_' + \
            self.period + '.csv'

        sites_infos_file = self.config.data_store_settings[
            'path'] + '/' + self.config.DB['db_name'] +\
            '_sites_infos' + '.csv'

        forecasts_set = self.config.data_store_settings['path'] +\
            '/' + self.config.DB['db_name'] + \
            '_forecasts_set_' + \
            self.period + '.csv'

        self.file_names = dict(training_set=training_set,
                               forecasts_set=forecasts_set,
                               sites_info=sites_infos_file)
