import os.path
import logging
import argparse as ap
from datetime import datetime, timedelta
import pandas as pd
import app.business_logic.model.db_manager as db_manager
import app.business_logic.service.merge_service as merge_service
from app.business_logic.model.config_manager import Config_manager
from app.business_logic.helper.calendar import add_calendar_fields
import app.business_logic.helper.regularizer as regularizer


config_manager = Config_manager()

fileDir = os.path.dirname(os.path.abspath(__file__))


def get_file_path(store_name):
    filename = os.path.join(
        fileDir, '../../' + store_name)
    filename = os.path.abspath(os.path.realpath(filename))

    return filename


class Dataset(object):

    def __init__(self, name, parse_dates=[]):
        self._name = name
        self.set = None
        self.regularization_params = {}
        self.file_path = None
        self.parse_dates = parse_dates

    @property
    def name(self):
        return self._name

    def update_data(self, data):
        self.set = data

    def __repr__(self):
        return self.name


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
        self.period = self._get_period()
        self._set_ranges(intervals)
        self._init_datasets()
        self.db_manager = db_manager.DB_manager(db_params)
        self.db_params = db_params
        self.has_conversion = self._has_conversion()

    def __repr__(self):
        return ("{0.name}:{0.period}:[{0.train_from} to {0.train_to}][{0.predict_from} to {0.predict_to}]".format(self))

    def _init_datasets(self):
        train = Dataset('train', ['date', 'date_time'])
        forecasts = Dataset('forecasts')
        observed = Dataset('observed')
        sites_infos = Dataset('sites_infos')
        self.dataset = {'train': train,
                        'forecasts': forecasts,
                        'observed': observed,
                        'sites_infos': sites_infos}
        self._set_file_names(self.dataset)
        self.data = ap.Namespace(**self.dataset)

    def _set_file_names(self, dataset):
        for item in dataset.itervalues():
            item.file_path = self.get_path(item.name)

    def _get_period(self):
        raise(NotImplementedError)

    def get_data(self):
        self._get_set(self.data.train)

        self._get_set(self.data.forecasts)

    def _get_set(self, dataset):
        if self.file_exists(dataset):
            logging.info("File exists ! : {}".format(dataset.name))
            dataset.set = self._read_file(dataset)
        else:
            logging.info("File does not exist ! : {}".format(dataset.name))
            self._create_data(dataset)

    def _create_data(self, dataset):
        if dataset.name == "train":
            self.create_training_set()

        if dataset.name == "forecasts":
            self.create_forecasts_set()

    def create_training_set(self):
        logging.info("{0} preparing new training set...".format(self))

        merged = merge_service.merge_all_training(self)
        merged = add_calendar_fields(merged)
        merged = regularizer.regularize_training(merged)

        self.data.train.update_data(merged)

        self._save_file(self.data.train)

        logging.info("{0} : finished preparing training set".format(self))

    def create_forecasts_set(self):
        logging.info("{0} preparing new forecasts set...".format(self))
        merged = merge_service.merge_all_forecasts(self)
        merged = add_calendar_fields(merged)
        regularization_params = regularizer._set_regularization_params(
            self.data.train.set)
        print regularization_params['x_mean'].head
        print regularization_params['x_std'].head
        merged = regularizer.regularize_forecasts(
            merged, regularization_params)
        self.data.forecasts.update_data(merged)

        self._save_file(self.data.forecasts)

        logging.info("{0} : finished preparing forecasts set".format(self))

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
        return datetime.strptime(date, '%Y-%m-%d').date()

    def _has_conversion(self):
        return not self.db_manager.conversion.empty

    def get_path(self, set_name):
        path = config_manager.datastore_settings[
            'path'] + '/' + self.__repr__()
        return path + "_" + set_name + ".csv"

    def file_exists(self, dataset):
        file_path = get_file_path(dataset.file_path)
        file_exists = os.path.exists(file_path)
        is_file = os.path.isfile(file_path)

        return file_exists and is_file

    def _read_file(self, dataset):
        df = pd.read_csv(get_file_path(dataset.file_path),
                         parse_dates=dataset.parse_dates, sep=';', index_col=0)

        return df

    def _save_file(self, dataset):
        print get_file_path(dataset.file_path)
        dataset.set.to_csv(get_file_path(dataset.file_path),
                           encoding='utf-8', sep=';')

    def get_counts(self):
        df_counts = self.db_manager.counts
        df_counts['date'] = pd.to_datetime(df_counts.timestamp.dt.date)

        df_counts = self._filter_on_date(
            df_counts, self.train_from, self.train_to)
        df_counts = self._aggregate_counts(df_counts)

        df_counts = df_counts[['idbldsite',
                               'compensatedin', 'date',
                               'date_time']]
        return df_counts

    def _aggregate_counts(self, df_counts):
        raise(NotImplementedError)

    def _filter_on_date(self, df, date_from, date_to):
        if date_from is not None:
            df = df[(df.date >= date_from)]

        if date_to is not None:
            df = df[(df.date < date_to)]
        return df
