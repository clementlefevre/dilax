import os.path
from datetime import date, datetime, timedelta
import pandas as pd
from collections import namedtuple
import app.business_logic.model.db_manager as db_manager
from app.business_logic.model.config_manager import Config_manager


config_manager = Config_manager()


class Datastore(object):

    def __init__(self, db_params=None, intervals={}, sites=None):
        """Summary

        Args:
            db_params (None, optional): db parameters as dict
            intervals (None, optional): dict format like
            {'train_from':'2016-01-01','train_to':'2016-12-01',\
             'predict_from':'2016-12-15'}

            period (None, optional): period of the datastore (currently implemented : day, hour)
            sites (None, optional): list of sites id. If None, all sites of the DB are taken into account.
        """
        self.name = db_params['db_name']
        self.db_manager = db_manager.DB_manager(db_params)
        self.period = self._get_period()
        self.db_params = db_params
        self._set_ranges(intervals)
        self.has_conversion = self._has_conversion()
        self.observed_targets = pd.DataFrame()
        self._set_file_names()

    def __repr__(self):
        return ("{0.name}:{0.period}:[{0.train_from} to {0.train_to}][{0.predict_from} to {0.predict_to}]".format(self))

    def _get_period(self):
        raise(NotImplementedError)

    def get_data(self):
        self._training_set_()
        self._create_sites_dict()
        self._get_observed_target()
        self._create_forecasts()

    def _set_ranges(self, interval):

        if 'train_from' in interval:
            self.train_from = self._set_dates(interval['train_from'])
        else:
            self.train_from = None

        if 'predict_from' in interval:
            self.predict_from = self._set_dates(interval['predict_from'])
        else:
            self.predict_from = datetime.now().date()

        if 'train_to' in interval:
            self.train_to = self._set_dates(interval['train_to'])
        else:
            self.train_to = self.predict_from - timedelta(days=1)

        if 'predict_to' in interval:
            self.predict_to = self._set_dates(interval['predict_to'])
        else:
            self.predict_to = self.predict_from + \
                timedelta(days=self.PREDICT_RANGE_DAYS)

    def _set_dates(self, date):
        return datetime.strptime(self.date_from, '%Y-%m-%d').date()

    def _has_conversion(self):
        return not self.db_manager.conversion.empty

    def _set_file_names(self):

        file_data = namedtuple(
            "File_data", ("path", "parse_dates"))
        path = config_manager.datastore_settings[
            'path'] + '/' + self.__repr__()
        training_set_path = path + '_training_set.csv'
        sites_infos_file_path = path + '_sites_infos.csv'
        forecasts_set_path = path + '_forecasts_set.csv'
        observed_set_path = path + '_observed_set.csv'

        self.file_names = dict(training_set=file_data(training_set_path,
                                                      ['date', 'date_time']),
                               forecasts_set=file_data(forecasts_set_path, []),
                               sites_infos=file_data(
            sites_infos_file_path, []),
            observed_set=file_data(observed_set_path, []))

    def file_exists(self, set_name):

        return os.path.isfile(self.file_names[set_name].path)

    def read_file(self, set_name):
        return pd.read_csv(self.file_names[set_name].path,
                           parse_dates=self.file_names[set_name].parse_dates)
