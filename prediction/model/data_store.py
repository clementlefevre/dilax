import pandas as pd
from datetime import datetime, timedelta

from db_manager import DB_manager
from config_manager import Config_manager
from helper.data_merger import merge_tables
from helper.data_helper import add_calendar_fields, regularize
from service.predictor_service import create_forecasts_data

config_manager = Config_manager()


class Data_store(object):
    def __init__(self, db_params, date_from=None, date_to=None,
                 period='D', create=False):
        self.name = db_params['db_name']
        self.create = create
        self.db_params = db_params
        self.period = period
        self._set_dates(date_from, date_to)
        self._set_file_names()

    def __repr__(self):
        return self.name, self.period, self.date_from, self.date_to

    def get_data(self):
        self._training_set_()
        self.create_sites_dict()

    def _training_set_(self):

        if self.create:
            print "prepare new training set..."
            self.db = DB_manager(self.db_params)
            self.training_data = merge_tables(self)
            self.training_data = add_calendar_fields(self.training_data)
            self.training_data = regularize(
                self, self.training_data)
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
        training_set = config_manager.data_store_settings['path'] +\
            '/' + self.name + \
            '_training_set_' + \
            self.period + '.csv'

        sites_infos_file = config_manager.data_store_settings[
            'path'] + '/' + self.name +\
            '_sites_infos' + '.csv'

        forecasts_set = config_manager.data_store_settings['path'] +\
            '/' + self.name + \
            '_forecasts_set_' + \
            self.period + '.csv'

        self.file_names = dict(training_set=training_set,
                               forecasts_set=forecasts_set,
                               sites_info=sites_infos_file)

    def _set_dates(self, date_from, date_to):
        if date_from is None:
            self.date_from = datetime.now().date()
        else:
            self.date_from = datetime.strptime(date_from, '%Y-%M-%d')

        if date_to is None:
            self.date_to = datetime.now().date() + timedelta(days=30)
        else:
            self.date_to = datetime.strptime(date_from, '%Y-%M-%d')
