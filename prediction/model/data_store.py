from db_manager import DB_manager
from config_manager import Config_manager

from helper.data_merger import merge_tables
from helper.data_helper import add_calendar_fields, regularize


class Data_store(object):
    def __init__(self, name, db_params={}, period='day'):
        self.name = name
        self.config = Config_manager()
        if not db_params:
            db_params = self.config.DB
        self.db = DB_manager(db_params)
        self.period = period
        self.__training_set()

    def __training_set(self):
        print "prepare training set..."
        self.data = merge_tables(self)
        self.data = add_calendar_fields(self)
        self.data = regularize(self)
        sefl.data.to_csv('data/store/training_set_' +
                         self.period + "_" + self.config.DB['db_name'] + '.csv')
        print "finished preparing training set"

    def create_predictors(date_from, date_to, period='day'):
