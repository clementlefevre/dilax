import pytest
from model.config_manager import Config_manager


@pytest.fixture(autouse=True, scope="module")
def config_manager():
    return Config_manager()


def test_config_params(config_manager):
    assert "DWE" in config_manager.DB['db_name']
    assert config_manager.DB['db_user'] == 'dwe-closed'
    assert not config_manager.features['month_12']
    assert config_manager.features['compensatedin']
    assert int(config_manager.DB['db_port']) == 5432


def test_config_DB(config_manager):
    assert config_manager.DB['db_port'] == "5432"
