from app.settings import DATASTORE_BASE_DIR, REDIS_NAMESPACE, AdminCfg, DataStoreCfg
from config.config import (
    ADMIN_COMMANDS_CHANNEL,
    NAMESPACE,
)
from config.config import (
    DATASTORE_BASE_DIR as CFG_BASE_DIR,
)


def test_constants_mirrored_from_config():
    assert REDIS_NAMESPACE == NAMESPACE
    assert DATASTORE_BASE_DIR == CFG_BASE_DIR


def test_admin_cfg_channel_defaults_to_config_value():
    cfg = AdminCfg()
    assert cfg.commands_channel == ADMIN_COMMANDS_CHANNEL


def test_datastore_cfg_defaults_to_config_values():
    ds = DataStoreCfg()
    assert ds.namespace == NAMESPACE
    assert ds.base_dir == CFG_BASE_DIR
