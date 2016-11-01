import pandas as pd
from db_manager import DB_manager
from config_manager import Config_manager

from helper.data_merger import merge_tables
from helper.data_helper import add_calendar_fields, regularize
from service.predictors_service import create_forecasts_data


class Data_store(object):
    def __init__(self, predictor,db_params={}):
        print 'create', predictor.create
        self.db_name = predictor.db_name
        self.config = Config_manager()
        if not db_params:
            self.db_params = self.config.DB
        else:
            self.db_params = db_params

        self.period = predictor.period
        self._training_set_(predictor.create)

    def _training_set_(self, create):
        if create:
            print "prepare training set..."
            self.db = DB_manager(self.db_params)
            self.data = merge_tables(self)
            self.data = add_calendar_fields(self.data)
            self.data = regularize(self)
            self.data.to_csv('data/store/training_set_' +
                             self.period + "_" + self.config.DB['db_name'] + '.csv')
            print "finished preparing training set"
        else:
            self.data = pd.read_csv('data/store/training_set_' +
                                    self.period + "_" + self.db_name + '.csv')

    def create_predictors(self, date_from, date_to):
        self.predictors = create_forecasts_data(
            self, date_from, date_to)
