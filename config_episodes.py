"""Root-level config_episodes shim.

Цей файл на верхньому рівні просто реекспортує реальні конфіги з
`ep_2.config_episodes`, щоб існуючий код із імпортом
`from config_episodes import EPISODE_CONFIG ...` не ламався.

Усі актуальні значення редагуються в `ep_2/config_episodes.py`.
"""

try:  # основний шлях
    from ep_2.config_episodes import (  # type: ignore
        EPISODE_CONFIG,
        EPISODE_CONFIG_1M,
        EPISODE_CONFIG_5M,
        SYSTEM_SIGNAL_CONFIG,
    )
except Exception:  # noqa: BLE001
    # Мінімальний резервний варіант, якщо з якоїсь причини пакет не доступний
    EPISODE_CONFIG = {
        "symbol": "BTCUSDT",
        "timeframe": "1m",
        "limit": 1000,
        "move_pct_up": 0.02,
        "move_pct_down": 0.02,
        "min_bars": 5,
        "max_bars": 120,
        "close_col": "close",
        "ema_span": 50,
        "impulse_k": 6,
        "cluster_k": 3,
        "pad_bars": 3,
        "retrace_ratio": 0.33,
        "require_retrace": True,
        "adaptive_threshold": True,
        "save_results": True,
    }
    EPISODE_CONFIG_1M = EPISODE_CONFIG.copy()
    EPISODE_CONFIG_5M = {**EPISODE_CONFIG, "timeframe": "5m"}
    SYSTEM_SIGNAL_CONFIG = {"enabled": True, "lookback": 100}

__all__ = [
    "EPISODE_CONFIG",
    "EPISODE_CONFIG_1M",
    "EPISODE_CONFIG_5M",
    "SYSTEM_SIGNAL_CONFIG",
]
