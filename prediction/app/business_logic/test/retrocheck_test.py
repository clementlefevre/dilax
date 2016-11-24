from ..customers_config import all_db_params
from ..model.datastore import Datastore


def test_retrocheck_datastore_day():
    datastore = Datastore(
        all_db_params[1], create=True, dt_from='2016-11-18', dt_to='2016-11-23', period="D", retrocheck=True)
    datastore.get_data()
    datastore.create_forecasts()
    print datastore.get_forecasts_set(2).head()
    assert datastore.get_forecasts_set(2).shape[0] > 0
    print datastore.observed_targets['counts'].head(30)
    print datastore.observed_targets['conversion'].head(30)
    assert datastore.observed_targets['counts'].shape[0] > 0
