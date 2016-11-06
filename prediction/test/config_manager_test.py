import pytest
from model.config_manager import Config_manager


@pytest.fixture(autouse=True, scope="module")
def config_manager():
    return Config_manager()


def test_config_params(config_manager):

    assert not config_manager.features['month_12']
    assert config_manager.features['compensatedin']
