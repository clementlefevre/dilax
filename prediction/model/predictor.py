from datetime import datetime, timedelta
from model.data_store import Data_store
from service.predictor_service import create_predictor


class Predictor(object):
    def __init__(self, db_name, date_from=None, date_to=None,
                 period='D', create=False):

        self.__set_dates(date_from, date_to)
        self.db_name = db_name
        self.period = period
        self.datastore = Data_store(db_name, create=create)
        self.predictor_dict = {}

    def __set_dates(self, date_from, date_to):
        if date_from is None:
            self.date_from = datetime.now().date()
        if date_to is None:
            self.date_to = datetime.now().date() + timedelta(days=30)


