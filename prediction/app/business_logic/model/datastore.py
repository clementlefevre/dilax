import pandas as pd
from datetime import datetime, timedelta
from model import logging
from db_manager import DB_manager
from config_manager import Config_manager
from helper.data_merger import merge_tables
from helper.data_helper import add_calendar_fields, regularize
from service.predictor_service import create_forecasts_data

config_manager = Config_manager()


class Datastore(object):
    def __init__(self, db_params, dt_from=None, dt_to=None,
                 period='D', create=False):
        self.name = db_params['db_name']
        self.create = create
        self.db_params = db_params
        self.period = period
        self._set_dates(dt_from, dt_to)
        self._set_file_names()

    def __repr__(self):
        return "{0.name}:period:{0.period}:[{0.date_from} to {0.date_to}]".format(self)

    def get_data(self):
        self._training_set_()
        self.create_sites_dict()

    def create_forecasts(self):
        if self.create:
            self.forecasts = create_forecasts_data(self)
            self.forecasts.to_csv(
                self.file_names['forecasts_set'], encoding='utf-8', sep=';')
            logging.info("{0} : forecasts_set saved to : {1}".format(
                self, self.file_names['forecasts_set']))
        else:
            self.forecasts = pd.read_csv(
                "data/store/" + self.name + "_forecasts_set_" + self.period + ".csv", sep=";", parse_dates=['date', 'date_time'])

    def get_training_set(self, site_id):
        return self.training_data[self.training_data.idbldsite == site_id]

    def get_forecasts_set(self, site_id):
        return self.forecasts[self.forecasts.idbldsite == site_id]

    def _training_set_(self):

        if self.create:
            logging.info("{0} prepare new training set...".format(self))
            self.db = DB_manager(self.db_params)
            self.public_holidays = self.db.public_holidays
            self.training_data = merge_tables(self)
            self.training_data = add_calendar_fields(self.training_data)
            self.training_data = regularize(
                self, self.training_data)
            self._save_training_set()

            logging.info("{0} : finished preparing training set".format(self))
        else:
            try:

                self.training_data = pd.read_csv(
                    self.file_names['training_set'], sep=";", parse_dates=['date', 'date_time'])

                self.public_holidays = pd.read_csv(
                    'data/store/' + self.db_params['db_name'] + '_public_holidays.csv', parse_dates=['day'], sep=";")

            except IOError as e:
                logging.error("{0} : error by reading the csv file : {1}".
                              format(self, self.file_names['training_set']))
                logging.error(e.message)

            try:
                self.sites_infos = pd.read_csv(
                    self.file_names['sites_info'], sep=";")
            except IOError as e:
                logging.error("{0} : error by reading the csv file : {1}".
                              format(self, self.file_names['sites_info']))
                logging.error(e.message)
            logging.info("{} : finished reading training set".format(self))

    def create_sites_dict(self):
        if self.create:
            sites_infos = self.sites_infos
        else:
            sites_infos = pd.read_csv(
                "data/store/" + self.name + "_sites_infos.csv", sep=";")

        self.sites_infos_dict = sites_infos.set_index(
            'idbldsite').T.to_dict()

    def _set_file_names(self):
        training_set = config_manager.datastore_settings[
            'path'] + '/' + self.name + '_training_set_' + self.period + '.csv'

        sites_infos_file = config_manager.datastore_settings[
            'path'] + '/' + self.name + '_sites_infos' + '.csv'

        forecasts_set = config_manager.datastore_settings[
            'path'] + '/' + self.name +\
            '_forecasts_set_' + self.period + '.csv'

        self.file_names = dict(training_set=training_set,
                               forecasts_set=forecasts_set,
                               sites_info=sites_infos_file)

    def _set_dates(self, date_from, date_to):
        if date_from is None:
            self.date_from = datetime.now().date()
        else:
            self.date_from = datetime.strptime(date_from, '%Y-%m-%d')

        if date_to is None:
            self.date_to = datetime.now().date() + timedelta(days=30)
        else:
            self.date_to = datetime.strptime(date_to, '%Y-%m-%d').date()

    def _save_training_set(self):
        self.training_data.to_csv(
            self.file_names['training_set'], encoding='utf-8', sep=';')
        self.sites_infos.to_csv(
            self.file_names['sites_info'], encoding='utf-8', sep=';')

        logging.info("{0} : training_set saved to : {1}".format(
            self, self.file_names['training_set']))
        logging.info("{0} : sites_infos saved to : {1}".format(
            self, self.file_names['sites_info']))
