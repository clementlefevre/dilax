import pytest
from ..model.db_manager import DB_manager


@pytest.fixture(autouse=True, scope="function")
def db_manager():
    db_params = {'db_user': 'dwe-closed', 'db_name': 'DWE_CLOSED_2013',
                 'db_port': '5432', 'db_pwd': '6EVAqWxOsX2Ao', 'db_url': 'localhost'}
    return DB_manager(db_params=db_params)


def test_connection(db_manager):
    assert db_manager.engine is not None
    db_manager.conversion.to_csv("data/test_conversion.csv", sep=";")


def test_queries(db_manager):
    sites = db_manager.sites
    assert len(sites) > 0
    addresses = db_manager.addresses
    assert len(addresses) > 0
