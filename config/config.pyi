import builtins
from typing import Any

NAMESPACE: str
DATASTORE_BASE_DIR: str
DEFAULT_LOOKBACK: int
DEFAULT_TIMEFRAME: str
MIN_READY_PCT: float
MAX_PARALLEL_STAGE2: int
TRADE_REFRESH_INTERVAL: int

BUY_SET: set[str]
SELL_SET: set[str]

REDIS_CHANNEL_ASSET_STATE: str
REDIS_SNAPSHOT_KEY: str
REDIS_CHANNEL_UI_ASSET_STATE: str
REDIS_SNAPSHOT_UI_KEY: str
ADMIN_COMMANDS_CHANNEL: str
STATS_CORE_KEY: str
STATS_HEALTH_KEY: str

# Core document (Stage3 metrics) and JSON paths
REDIS_DOC_CORE: str
REDIS_CORE_PATH_TRADES: str
REDIS_CORE_PATH_STATS: str
REDIS_CORE_PATH_HEALTH: str

# TTLs and feature toggles for migration
UI_SNAPSHOT_TTL_SEC: int
CORE_TTL_SEC: int
CORE_DUAL_WRITE_OLD_STATS: bool
UI_PAYLOAD_SCHEMA_VERSION: str
UI_TP_SL_FROM_STAGE3_ENABLED: bool
UI_USE_V2_NAMESPACE: bool
UI_DUAL_PUBLISH: bool
SIMPLE_UI_MODE: bool

UI_LOCALE: str
UI_COLUMN_VOLUME: str
TICK_SIZE_MAP: dict[str, float]
TICK_SIZE_BRACKETS: list[tuple[float, float]]
TICK_SIZE_DEFAULT: float

STAGE2_STATUS: dict[str, str]
ASSET_STATE: dict[str, str]

# Canonical keys across pipeline (Stage1 → Stage2 → UI)
K_SYMBOL: str
K_SIGNAL: str
K_TRIGGER_REASONS: str
K_RAW_TRIGGER_REASONS: str
K_STATS: str
K_THRESHOLDS: str

# Stage2 output keys
K_MARKET_CONTEXT: str
K_RECOMMENDATION: str
K_CONFIDENCE_METRICS: str
K_ANOMALY_DETECTION: str
K_RISK_PARAMETERS: str

TRIGGER_TP_SL_SWAP_LONG: str
TRIGGER_TP_SL_SWAP_SHORT: str
TRIGGER_SIGNAL_GENERATED: str
TRIGGER_NAME_MAP: dict[str, str]

# Semaphores (runtime types), kept as Any for typing simplicity
OI_SEMAPHORE: Any
KLINES_SEMAPHORE: Any
DEPTH_SEMAPHORE: Any
REDIS_CACHE_TTL: int

class AssetFilterError(Exception): ...

class SymbolInfo:
    symbol: str
    status: str
    base_asset: str
    quote_asset: str
    contract_type: str
    def dict(self, *args: Any, **kwargs: Any) -> builtins.dict[str, Any]: ...
    def __init__(
        self,
        *,
        symbol: str,
        status: str,
        base_asset: str,
        quote_asset: str,
        contract_type: str,
        **kwargs: Any,
    ) -> None: ...

class FilterParams:
    min_quote_volume: float
    min_price_change: float
    min_open_interest: float
    min_orderbook_depth: float
    min_atr_percent: float
    max_symbols: int
    dynamic: bool
    strict_validation: bool
    def dict(self, *args: Any, **kwargs: Any) -> builtins.dict[str, Any]: ...
    def __init__(
        self,
        *,
        min_quote_volume: float,
        min_price_change: float,
        min_open_interest: float,
        min_orderbook_depth: float,
        min_atr_percent: float,
        max_symbols: int,
        dynamic: bool = ...,
        strict_validation: bool = ...,
        **kwargs: Any,
    ) -> None: ...

class MetricResults:
    initial_count: int
    prefiltered_count: int
    filtered_count: int
    result_count: int
    elapsed_time: float
    params: builtins.dict[str, Any]
    def dict(self, *args: Any, **kwargs: Any) -> builtins.dict[str, Any]: ...
    def __init__(
        self,
        *,
        initial_count: int,
        prefiltered_count: int,
        filtered_count: int,
        result_count: int,
        elapsed_time: float,
        params: builtins.dict[str, Any],
        **kwargs: Any,
    ) -> None: ...

ASSET_CLASS_MAPPING: dict[str, list[str]]
STAGE2_CONFIG: dict[str, Any]
OPTUNA_PARAM_RANGES: dict[str, tuple]
STAGE2_RANGE_PARAMS: dict[str, float]
STAGE3_TRADE_PARAMS: dict[str, float]
STAGE2_AUDIT: dict[str, float | int | str | bool]
WS_GAP_BACKFILL: dict[str, int | bool]

INTERVAL_TTL_MAP: dict[str, int]

STAGE1_PREFILTER_THRESHOLDS: dict[str, float | int]
MANUAL_FAST_SYMBOLS_SEED: list[str]
USER_SETTINGS_DEFAULT: dict[str, str]
PRELOAD_1M_LOOKBACK_INIT: int
SCREENING_LOOKBACK: int
PRELOAD_DAILY_DAYS: int
FAST_SYMBOLS_TTL_MANUAL: int
FAST_SYMBOLS_TTL_AUTO: int
PREFILTER_INTERVAL_SEC: int
CACHE_TTL_DAYS: int
OPTUNA_SQLITE_URI: str
MIN_CONFIDENCE_TRADE_PLACEHOLDER: float
STAGE1_MONITOR_PARAMS: dict[str, float | int]
PREFILTER_BASE_PARAMS: dict[str, float | int | bool]

# Feature flags for Stage1 logic
USE_VOL_ATR: bool
USE_RSI_DIV: bool
USE_VWAP_DEVIATION: bool

__all__: list[str]
