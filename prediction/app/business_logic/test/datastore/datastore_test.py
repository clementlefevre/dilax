from datetime import datetime, timedelta
import app.business_logic.model.datastore.day as daystore
import app.business_logic.customers_config


def test_datastore_creation():
    db_params = app.business_logic.customers_config.all_db_params[1]
    dayDatastore = daystore.DayDatastore(db_params=db_params)
    assert dayDatastore.PREDICT_RANGE_DAYS == 30
    assert dayDatastore.train_from is None

    assert dayDatastore.train_to == datetime.now().date() - timedelta(days=1)
    assert dayDatastore.predict_from == datetime.now().date()
    assert dayDatastore.predict_to == datetime.now().date() + timedelta(days=30)
    assert dayDatastore.file_names[
        'training_set'].path == "data/store/DWE_CLOSED_2013:day:[None to 2016-11-24][2016-11-25 to 2016-12-25]_training_set.csv"


def test_datastore_conversion():
    db_params = app.business_logic.customers_config.all_db_params[2]
    dayDatastore = daystore.DayDatastore(db_params=db_params)
    assert not dayDatastore.has_conversion
