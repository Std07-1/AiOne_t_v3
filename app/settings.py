"""Конфігураційні моделі застосунку (Redis, DataStore, Prometheus, Admin).

Шлях: ``app/settings.py``

Використовує pydantic для декларативних моделей та YAML-файл для DataStore частини.

Уніфікація: базові константи (namespace, base_dir, admin channel) тягнемо з
`config.config` як єдиного джерела правди.
"""

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from config.config import (
    DATASTORE_BASE_DIR as CFG_DATASTORE_BASE_DIR,
)
from config.config import (
    NAMESPACE as CFG_NAMESPACE,
)

load_dotenv()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # ігноруємо невідомі змінні замість ValidationError
    )

    redis_host: str = "localhost"
    redis_port: int = 6379
    binance_api_key: str | None = None
    binance_secret_key: str | None = None
    telegram_token: str | None = None
    admin_id: int = 0
    # Додаткові (не критичні) змінні середовища для сумісності зі старою конфігурацією
    log_level: str | None = None
    log_to_file: bool | None = None
    database_url: str | None = None
    db_host: str | None = None
    db_port: int | None = None
    db_user: str | None = None
    db_password: str | None = None
    db_name: str | None = None

    # Проста валідація полів перенесена на рівень запуску/конфігів; додаткові
    # pydantic-валідатори не використовуємо тут для сумісності зі stubs mypy.


settings = Settings()  # буде валідовано під час імпорту

# Уніфіковані значення з config.config
REDIS_NAMESPACE = CFG_NAMESPACE
DATASTORE_BASE_DIR = CFG_DATASTORE_BASE_DIR
RAM_BUFFER_MAX_BARS = (
    30000  # Максимальна кількість барів у RAMBuffer на symbol/timeframe
)


class Profile(BaseModel):
    name: str = "small"
    ram_limit_mb: int = 512
    max_symbols_hot: int = 96
    hot_ttl_sec: int = 6 * 3600
    warm_ttl_sec: int = 24 * 3600
    flush_batch_max: int = 8
    flush_queue_soft: int = 200
    flush_queue_hard: int = 1000


class PrometheusCfg(BaseModel):
    enabled: bool = True
    port: int = 9109
    path: str = "/metrics"


class TradeUpdaterCfg(BaseModel):
    skipped_ewma_alpha: float = 0.3
    backoff_multiplier: float = 1.5
    max_backoff_sec: int = 300
    drift_warn_high: float = 2.5
    drift_warn_low: float = 0.5
    pressure_warn: float = 2.0  # skipped_ewma / active_trades
    cycle_histogram_buckets: list[float] = Field(
        default_factory=lambda: [0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300]
    )
    # ── Optional optimization hooks (disabled by default) ──
    auto_interval_scale_enabled: bool = (
        False  # enable adaptive interval scaling when pressure stays high
    )
    auto_interval_scale_cycles: int = (
        3  # how many consecutive high-pressure cycles before scaling interval
    )
    auto_interval_scale_factor: float = (
        1.25  # multiplier applied to dynamic_interval when triggered
    )
    auto_interval_scale_cap: float = 900.0  # hard cap for scaled interval

    auto_alpha_enabled: bool = False  # adapt skipped_ewma_alpha based on turbulence
    alpha_min: float = 0.05
    alpha_max: float = 0.6
    alpha_step: float = 0.05  # step to increase/decrease
    alpha_turbulence_drift: float = (
        2.0  # drift threshold to consider turbulent (above high warn still counts)
    )
    alpha_turbulence_pressure: float = (
        1.5  # pressure threshold to consider turbulent (below pressure_warn for pre-empt)
    )
    alpha_calm_drift: float = 1.05  # drift below this AND pressure low => calm
    alpha_calm_pressure: float = 0.5
    alpha_calm_cycles: int = (
        5  # consecutive calm cycles before lowering alpha (longer memory)
    )

    skip_reasons_top_n: int = 5  # number of top skip reasons to publish (if enabled)
    publish_skip_reasons: bool = False

    dynamic_priority_enabled: bool = (
        False  # future: temporarily drop low-priority symbols under pressure
    )
    dynamic_priority_min_active: int = 10  # don't drop if active universe already small


class AdminCfg(BaseModel):
    enabled: bool = True
    # Уникаємо прямого імпорту константи під час імпорту модуля,
    # щоб не створювати крихкі залежності; значення еквівалентне
    # config.config.ADMIN_COMMANDS_CHANNEL
    commands_channel: str = f"{CFG_NAMESPACE}:admin:commands"
    health_ping_sec: int = 30


class DataStoreCfg(BaseModel):
    namespace: str = REDIS_NAMESPACE
    base_dir: str = DATASTORE_BASE_DIR
    profile: Profile = Profile()
    trade_updater: TradeUpdaterCfg = TradeUpdaterCfg()
    intervals_ttl: dict[str, int] = Field(
        default_factory=lambda: {
            "1m": 21600,
            "5m": 43200,
            "15m": 86400,
            "1h": 259200,
            "4h": 604800,
            "1d": 2592000,
        }
    )
    write_behind: bool = True
    validate_on_write: bool = True
    validate_on_read: bool = True
    io_retry_attempts: int = 3
    io_retry_backoff: float = 0.25
    prometheus: PrometheusCfg = PrometheusCfg()
    admin: AdminCfg = AdminCfg()


def load_datastore_cfg(path: str = "config/datastore.yaml") -> DataStoreCfg:
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return DataStoreCfg(**data)
