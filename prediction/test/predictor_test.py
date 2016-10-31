import pytest
from model.predictor import Predictor
from datetime import datetime, timedelta


@pytest.fixture(autouse=True, scope="module")
def predictor():
    return Predictor("DWE_CLOSED", create=False)


def test_predictor_period(predictor):
    assert predictor.period == "D"
    assert predictor.date_from == datetime.now().date()
    assert predictor.datastore.data.shape[0] > 0
