"""Конфігураційні моделі застосунку (Redis, DataStore, Prometheus, Admin).

Шлях: ``app/settings.py``

Використовує pydantic для декларативних моделей та YAML-файл для DataStore частини.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator
from typing import Dict, Optional
import yaml

load_dotenv()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # ігноруємо невідомі змінні замість ValidationError
    )

    redis_host: str = "localhost"
    redis_port: int = 6379
    binance_api_key: Optional[str] = None
    binance_secret_key: Optional[str] = None
    telegram_token: Optional[str] = None
    admin_id: int = 0
    # Додаткові (не критичні) змінні середовища для сумісності зі старою конфігурацією
    log_level: Optional[str] = None
    log_to_file: Optional[bool] = None
    database_url: Optional[str] = None
    db_host: Optional[str] = None
    db_port: Optional[int] = None
    db_user: Optional[str] = None
    db_password: Optional[str] = None
    db_name: Optional[str] = None

    @field_validator("redis_host", "redis_port")
    @classmethod
    def _required(cls, v, info):  # type: ignore[override]
        if v in (None, "", 0):
            raise ValueError(f"{info.field_name} is required")
        return v


settings = Settings()  # буде валідовано під час імпорту

REDIS_NAMESPACE = "ai_one"
DATASTORE_BASE_DIR = "./datastore"
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


class AdminCfg(BaseModel):
    commands_channel: str = "ai_one:admin:commands"
    health_ping_sec: int = 30


class DataStoreCfg(BaseModel):
    namespace: str = "ai_one"
    base_dir: str = "./datastore"
    profile: Profile = Profile()
    intervals_ttl: Dict[str, int] = Field(
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
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return DataStoreCfg(**data)
