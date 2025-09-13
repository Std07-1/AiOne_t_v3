"""Менеджер стану активів (спрощена версія без калібрування).

Шлях: ``app/asset_state_manager.py``

Призначення:
    • централізоване зберігання стану активів (signal / thresholds / stats);
    • легкі геттер-и для UI/Stage2 (alerts, всі активи);
    • без історичної логіки калібрування (видалено).
"""

import logging
from datetime import datetime
from rich.console import Console
from rich.logging import RichHandler

from typing import Any, Dict, List, Optional
from config.config import STAGE2_STATUS, ASSET_STATE

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("app.asset_state_manager")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


class AssetStateManager:
    """Централізований менеджер стану активів без підтримки калібрування."""

    def __init__(
        self,
        initial_assets: List[str],
        cache_handler: Optional[Any] = None,
        symbol_cfg: Optional[Dict[str, Any]] = None,
    ):
        self.state: Dict[str, Dict[str, Any]] = {}
        self.cache: Optional[Any] = cache_handler
        self._symbol_cfg: Dict[str, Any] = symbol_cfg or {}
        for asset in initial_assets:
            self.init_asset(asset)

    def set_cache_handler(self, cache_handler: Any) -> None:
        """Встановити обробник кешу/сховища порогів для збереження калібрування."""
        self.cache = cache_handler

    def set_symbol_config(self, symbol_cfg: Dict[str, Any]) -> None:
        """Встановити локальну мапу конфігів порогів на символ (in-memory)."""
        self._symbol_cfg = symbol_cfg or {}

    def init_asset(self, symbol: str):
        """Ініціалізація базового стану для активу"""
        self.state[symbol] = {
            "symbol": symbol,
            "signal": "NONE",
            "trigger_reasons": [],
            "confidence": 0.0,
            "hints": ["Очікування даних..."],
            "tp": None,
            "sl": None,
            "cluster_factors": [],
            "stats": {},
            "state": ASSET_STATE["INIT"],
            "stage2": False,
            "stage2_status": STAGE2_STATUS["PENDING"],
            "last_updated": datetime.utcnow().isoformat(),
            "visible": True,
        }

    def update_asset(self, symbol: str, updates: Dict[str, Any]):
        """Оновлення стану активу з мерджем існуючих даних"""
        if symbol not in self.state:
            self.init_asset(symbol)

        current = self.state[symbol]
        # Нормалізація trigger_reasons якщо приходить None
        if "trigger_reasons" in updates and updates["trigger_reasons"] is None:
            updates["trigger_reasons"] = []
        self.state[symbol] = {
            **current,
            **updates,
            "last_updated": datetime.utcnow().isoformat(),
        }

    def get_all_assets(self) -> List[Dict[str, Any]]:
        """Отримати всі активи для відображення в UI"""
        if not self.state:
            logger.warning("Стан активів порожній, немає даних для відображення")
            return []

        return list(self.state.values())

    def get_alert_signals(self) -> List[Dict[str, Any]]:
        """Отримати сигнали ALERT* (ALERT/ALERT_BUY/ALERT_SELL) для Stage2."""
        return [
            asset
            for asset in self.state.values()
            if str(asset.get("signal", "")).upper().startswith("ALERT")
        ]

    # update_calibration видалено — калібрування не підтримується
