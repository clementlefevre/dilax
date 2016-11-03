from datetime import datetime, timedelta
from model.data_store import Data_store
from service.predictor_service import create_forecasts_data


class Predictor(object):
    def __init__(self, db_name, date_from=None, date_to=None,
                 period='D', create=False):

        self._set_dates(date_from, date_to)
        self.db_name = db_name
        self.period = period
        self.create = create
        print "self", self
        self.datastore = Data_store(self)

    def _set_dates(self, date_from, date_to):
        if date_from is None:
            self.date_from = datetime.now().date()
        else:
            self.date_from = datetime.strptime(date_from, '%Y-%M-%d')

        if date_to is None:
            self.date_to = datetime.now().date() + timedelta(days=30)
        else:
            self.date_to = datetime.strptime(date_from, '%Y-%M-%d')

    def __repr__(self):
        return self.db_name

    def create_predictors(self):
        create_forecasts_data(self)
