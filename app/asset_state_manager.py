# asset_state_manager.py
import asyncio
import logging
from datetime import datetime
from rich.console import Console
from rich.logging import RichHandler

from typing import Any, Dict, List, Optional
from app.thresholds import Thresholds, save_thresholds

# --- Налаштування логування ---
logger = logging.getLogger("app.asset_state_manager")
logger.setLevel(logging.INFO)  # Змінено на INFO для зменшення шуму
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


class AssetStateManager:
    """Централізований менеджер стану активів з підтримкою калібрування.

    Забезпечує:
    - базовий стан активів,
    - асинхронні події калібрування,
    - інтеграцію з кешем/сховищем порогів та локальною мапою конфігів символів.

    Args:
        initial_assets: Початковий список символів.
        cache_handler: Опціональний кеш/сховище для збереження порогів.
        symbol_cfg: Опціональна мапа локальних конфігів порогів на символ.
    """

    def __init__(
        self,
        initial_assets: List[str],
        cache_handler: Optional[Any] = None,
        symbol_cfg: Optional[Dict[str, Any]] = None,
    ):
        self.state: Dict[str, Dict[str, Any]] = {}
        self.calibration_events: Dict[str, asyncio.Event] = {}
        self.cache: Optional[Any] = (
            cache_handler  # може бути задано пізніше через сеттер
        )
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
            "state": "init",
            "stage2": False,
            "stage2_status": "pending",
            "last_updated": datetime.utcnow().isoformat(),
            "visible": True,
            "calib_status": "pending",
            "last_calib": None,
            "calib_priority": "normal",  # +++ НОВЕ ПОЛЕ +++
            "calib_queued_at": None,
        }
        # Створюємо подію для очікування калібрування
        self.calibration_events[symbol] = asyncio.Event()

    async def wait_for_calibration(self, symbol: str, timeout: float = 120):
        """Асинхронно чекає завершення калібрування активу або таймауту"""
        event = self.calibration_events.get(symbol)
        if event is None:
            # Якщо подія не створена, ініціалізуємо актив
            self.init_asset(symbol)
            event = self.calibration_events[symbol]
        try:
            await asyncio.wait_for(event.wait(), timeout)
        except asyncio.TimeoutError:
            pass

    def update_asset(self, symbol: str, updates: Dict[str, Any]):
        """Оновлення стану активу з мерджем існуючих даних"""
        if symbol not in self.state:
            self.init_asset(symbol)

        current = self.state[symbol]
        # Додаємо статуси для відстеження термінових завдань
        if "calib_status" in updates:
            logger.debug(f"Updating {symbol} calib_status: {updates['calib_status']}")
        if "calib_status" in updates and updates["calib_status"] == "queued_urgent":
            updates["calib_priority"] = "urgent"
            updates["calib_queued_at"] = datetime.utcnow().isoformat()
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

    async def update_calibration(self, symbol: str, params: Dict[str, Any]):
        """Оновити калібрування символу та зберегти пороги у кеш/сховищі.

        Захищено від відсутності ``cache``/``_symbol_cfg``. Якщо кеш не задано,
        збереження в сховищі пропускається з попередженням.
        """
        # Генерація та (опційне) збереження порогів
        thr = Thresholds.from_mapping(params)
        if getattr(self, "cache", None) is not None:
            try:
                await save_thresholds(symbol, thr, self.cache)
            except Exception:
                logger.exception("Помилка збереження порогів для %s", symbol)
        else:
            logger.warning(
                "Cache handler не задано — пропускаємо збереження порогів для %s",
                symbol,
            )

        # Оновлення стану
        if symbol in self.state:
            self.state[symbol].update(
                {"calibrated_params": params, "calib_status": "completed"}
            )

        # Оновлення локальної мапи порогів (створимо/оновимо запис)
        try:
            if not hasattr(self, "_symbol_cfg") or self._symbol_cfg is None:
                self._symbol_cfg = {}
            self._symbol_cfg[symbol] = thr
        except Exception:
            logger.debug("Не вдалося оновити локальну мапу порогів для %s", symbol)

        # Сигнал про завершення
        if event := self.calibration_events.get(symbol):
            event.set()
