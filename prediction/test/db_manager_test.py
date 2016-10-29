import pytest
from model.db_manager import DB_manager
from config import *


@pytest.fixture(autouse=True, scope="module")
def db_manager():
    return DB_manager(DB_NAME, DB_USER, DB_PWD, DB_URL, DB_PORT)


def test_connection(db_manager):
    assert db_manager.engine is not None
    db_manager = DB_manager()
    assert db_manager.engine is not None


def test_queries(db_manager):
    sites = db_manager.sites
    assert len(sites) > 0
    addresses = db_manager.addresses
    assert len(addresses) > 0
