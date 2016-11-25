from ..model.datastore import Datastore
from ..model.merger_lib.counts import CountsMerger
from ..customers_config import all_db_params


def test_merger_counts():

    datastore = Datastore(all_db_params[1], create=True, dt_to='2017-01-15')
    datastore.get_data()
    merger = CountsMerger()
    merger.set_datastore(datastore)
    df_merged = merger.merge_and_clean()
    print df_merged.head()
    assert not df_merged.empty
