from model.data_store import Data_store
from model.db_manager import DB_manager


def test_missing_holidays():
    db_params = {'db_user': 'dwe-arcadia', 'db_name': 'DWE_ARCADIA_2015',
                 'db_port': '5432', 'db_pwd': 'VtJ5Cw3PKuOi4i3b',
                 'db_url': 'localhost'}

    db_manager = DB_manager(db_params)
    print db_manager.sites
    assert db_manager.engine is not None
