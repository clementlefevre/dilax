from datetime import datetime, timedelta
import app.business_logic.model.datastore.day as daystore
import app.business_logic.model.datastore.hour as hourstore

import app.business_logic.customers_config


db_params = app.business_logic.customers_config.all_db_params['DWE_CLOSED_2013']


def test_datastore_creation():

    dayDatastore = daystore.DayDatastore(db_params=db_params)
    assert dayDatastore.PREDICT_RANGE_DAYS == 30
    assert dayDatastore.train_from is None

    assert dayDatastore.train_to == datetime.now().date() - timedelta(days=1)
    assert dayDatastore.predict_from == datetime.now().date()
    assert dayDatastore.predict_to == datetime.now().date() + timedelta(days=30)
    assert not dayDatastore.file_exists('training_set')


def test_datastore_conversion():
    db_params = app.business_logic.customers_config.all_db_params[2]
    dayDatastore = daystore.DayDatastore(db_params=db_params)
    assert not dayDatastore.has_conversion


def test_datastore_training_set():
    dayDatastore = daystore.DayDatastore(db_params=db_params, intervals={
                                         'predict_from': '2016-11-21'})
    dayDatastore.get_data()
    assert not dayDatastore.data.train.set.empty


def test_datastore_hour():
    dayDatastore = hourstore.HourDatastore(db_params=db_params, intervals={
                                           'predict_from': '2016-11-21'})
    dayDatastore.get_data()
    assert not dayDatastore.data.train.set.empty
