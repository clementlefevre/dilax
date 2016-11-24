import os
import pandas as pd
from datetime import datetime, timedelta
from ..model import logging
from db_manager import DB_manager
from config_manager import Config_manager
from ..helper.data_merger import merge_tables
from ..helper.data_helper import add_calendar_fields, regularize
from ..helper.file_helper import get_file_path
from ..service.predictor_service import create_forecasts_data
from ..service.conversion_service import get_conversion
from ..service.counts_service import get_counts

config_manager = Config_manager()
fileDir = os.path.dirname(os.path.abspath(__file__))


class Datastore(object):
    def __init__(self, db_params, dt_from=None, dt_to=None,
                 period='D', create=False, retrocheck=False):
        self.name = db_params['db_name']
        self.db_params = db_params
        self.period = period
        self._set_dates(dt_from, dt_to)
        self.create = create
        self.retrocheck = retrocheck
        self.conversion = db_params['conversion']
        self.observed_targets = pd.DataFrame()
        self.no_weatherstore_sites = []
        self._set_file_names()

    def __repr__(self):
        return "{0.name}:period:{0.period}:retrocheck:{0.retrocheck}[{0.date_from} to\
         {0.date_to}]".format(self)

    def get_data(self):
        self._training_set_()
        self.create_sites_dict()
        if self.retrocheck:
            self.observed = self._get_observed_target()

    def create_forecasts(self):
        if self.create:
            self.forecasts = create_forecasts_data(self)
            self.forecasts.to_csv(get_file_path(
                self.file_names['forecasts_set'], fileDir),
                encoding='utf-8', sep=';')
            logging.info("{0} : forecasts_set saved to : {1}".format(
                self, self.file_names['forecasts_set']))
        else:
            forecasts_path = get_file_path(
                "data/store/" + self.name + "_forecasts_set_" +
                self.period + ".csv", fileDir)
            self.forecasts = pd.read_csv(
                forecasts_path, sep=";", parse_dates=['date', 'date_time'])

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
            print self.training_data.tail()
            self._save_training_set()

            logging.info("{0} : finished preparing training set".format(self))
        else:

            try:

                self.training_data = pd.read_csv(
                    get_file_path(self.file_names['training_set'],
                                  fileDir), sep=";",
                    parse_dates=['date', 'date_time'])

                holidays_file = get_file_path(
                    'data/store/' +
                    self.db_params['db_name'] + '_public_holidays.csv',
                    fileDir)
                self.public_holidays = pd.read_csv(
                    holidays_file, parse_dates=['day'], sep=";")

            except IOError as e:
                logging.error("{0} : error by reading the csv file : {1}".
                              format(self, self.file_names['training_set']))
                logging.error(e.message)

            try:
                self.sites_infos = pd.read_csv(get_file_path(
                    self.file_names['sites_info'], fileDir), sep=";")
            except IOError as e:
                logging.error("{0} : error by reading the csv file : {1}".
                              format(self, self.file_names['sites_info']))
                logging.error(e.message)
            logging.info("{} : finished reading training set".format(self))

    def create_sites_dict(self):
        if self.create:
            sites_infos = self.sites_infos
        else:
            filename = get_file_path(self.file_names['sites_info'], fileDir)
            sites_infos = pd.read_csv(filename, sep=";")
            print sites_infos.head()

        self.sites_infos_dict = sites_infos.set_index(
            'idbldsite').T.to_dict()

    def _set_file_names(self):
        training_set = config_manager.datastore_settings[
            'path'] + '/' + self.name + '_training_set_' + self.period\
            + "_retro_" + str(self.retrocheck) + '.csv'

        sites_infos_file = config_manager.datastore_settings[
            'path'] + '/' + self.name + '_sites_infos' + '.csv'

        forecasts_set = config_manager.datastore_settings[
            'path'] + '/' + self.name +\
            '_forecasts_set_' + self.period + \
            "_retro_" + str(self.retrocheck) + '.csv'

        observed_set = config_manager.datastore_settings[
            'path'] + '/' + self.name +\
            '_observed_set_' + self.period + \
            "_retro_" + str(self.retrocheck) + '.csv'

        self.file_names = dict(training_set=training_set,
                               forecasts_set=forecasts_set,
                               sites_info=sites_infos_file,
                               observed_set=observed_set)

    def _set_dates(self, date_from, date_to):
        if date_from is None:
            self.date_from = datetime.now().date()
        else:
            self.date_from = datetime.strptime(date_from, '%Y-%m-%d').date()

        if date_to is None and self.period == 'D':
            self.date_to = datetime.now().date() + timedelta(days=30)
        elif date_to is None and self.period == 'H':
            self.date_to = datetime.now().date() + timedelta(days=7)
        else:
            self.date_to = datetime.strptime(date_to, '%Y-%m-%d').date()

    def _save_training_set(self):
        self.training_data.to_csv(get_file_path(
            self.file_names['training_set'], fileDir), encoding='utf-8', sep=';')
        self.sites_infos.to_csv(get_file_path(
            self.file_names['sites_info'], fileDir), encoding='utf-8', sep=';')

        logging.info("{0} : training_set saved to : {1}".format(
            self, self.file_names['training_set']))
        logging.info("{0} : sites_infos saved to : {1}".format(
            self, self.file_names['sites_info']))

    def _get_observed_target(self):

        if self.create:

            counts = get_counts(self, date_from=self.date_from,
                                date_to=self.date_to)
            if self.conversion:
                conversions = get_conversion(self)
                if self.period == 'D':

                    conversions = conversions[(conversions.date.dt.date >=
                                               self.date_from) & (conversions.date.dt.date <= self.date_to)]
                    print ("counts*******************")
                    print counts
                    print ("conversions*******************")
                    print conversions
                    self.observed_targets = pd.merge(
                        counts, conversions, on=['date', 'idbldsite'], how='left')
                    print ("self.observed_targets*******************")
                    print self.observed_targets

                elif self.period == 'H':
                    if self.conversion:
                        conversions.rename(
                            columns={'timefrom': 'date_time'}, inplace=True)
                        conversions = conversions[(conversions.date_time.dt.date >=
                                                   self.date_from) & (conversions.date_time.dt.date <= self.date_to)]
                        self.observed_targets = pd.merge(
                            counts, conversions, on=['date_time', 'idbldsite'], how='left')
            else:
                self.observed_targets = counts
            self.observed_targets.to_csv(get_file_path(
                self.file_names['observed_set'], fileDir), encoding='utf-8', sep=';')

        else:
            self.observed_targets = pd.read_csv(
                get_file_path(self.file_names['observed_set'],
                              fileDir), sep=";",
                parse_dates=['date_time'])
