import pytest
from model.predictor import Predictor
from datetime import datetime, timedelta


@pytest.fixture(autouse=True, scope="module")
def predictor():
    return Predictor("DWE_CLOSED_2013", create=True)


def test_predictor_period(predictor):
    assert predictor.period == "D"
    assert predictor.date_from == datetime.now().date()
    assert predictor.datastore.data.shape[0] > 0


def test_datastore_create_forecasts(predictor):
    date_from = datetime.now().date()
    date_to = date_from + timedelta(days=60)

    predictor.create_predictors()
    assert predictor.forecasts.shape[0] > 0
