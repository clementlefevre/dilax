from db_manager import DB_manager

from helper.data_merger import merge_tables
from helper.data_helper import add_calendar_fields, regularize


class Data_store(object):
    def __init__(self, name, db_params={}, period='day'):
        self.name = name
        self.period = period
        self.db = DB_manager(**db_params)
        self.__create_data()

    def __create_data(self):
        print "creating data..."
        self.data = merge_tables(self)
        self.data = add_calendar_fields(self)
        self.data = regularize(self)

        print "finished creating data"
