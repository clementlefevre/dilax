import pytest

from ..model.datastore import Datastore
from ..model.prediction import Prediction

db_params = {'db_user': 'dwe-closed', 'db_name': 'DWE_CLOSED_2013',
             'db_port': '5432', 'db_pwd': '6EVAqWxOsX2Ao', 'db_url': 'localhost'}


# @pytest.fixture(autouse=True, scope="function")
# def datastore_D():

#     datastore = Datastore(db_params, period='D',
#                           create=True, dt_to='2016-12-15')

#     datastore.get_data()
#     datastore.create_forecasts()
#     return datastore


@pytest.fixture(autouse=True, scope="function")
def datastore_H():
    datastore = Datastore(db_params, period='H',
                          create=False, dt_to='2016-11-30')

    datastore.get_data()
    datastore.create_forecasts()
    return datastore


# def test_predictors_D(datastore_D):

#     predictor = Prediction(datastore_D)
#     predictor._create_X_Y_per_site(2, "volume")
#     assert datastore_D.get_training_set(2).shape[0] > 0


def test_predictors_H(datastore_H):
    predictor = Prediction(datastore_H)
    predictor._create_X_Y_per_site(2, "volume")
    assert datastore_H.get_training_set(2).shape[0] > 0


# def test_prediction_D(datastore_D):
#     predictor = Prediction(datastore_D)
#     predictor.make_prediction(2, "volume")


def test_prediction_H(datastore_H):
    predictor = Prediction(datastore_H)
    predictor.make_prediction(2, "compensatedtotalin")
