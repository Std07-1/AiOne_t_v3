"""Публікація агрегованого стану активів у Redis (UI snapshot).

Шлях: ``UI/publish_full_state.py``

Винос з `app.screening_producer` для розділення відповідальностей:
    • збір та нормалізація стану (producer)
    • публікація / форматування для UI (цей модуль)

Формат payload (type = REDIS_CHANNEL_ASSET_STATE):
    {
        "type": REDIS_CHANNEL_ASSET_STATE,
        "meta": {"ts": ISO8601UTC},
        "counters": {"assets": N, "alerts": A},
        "assets": [ { ... нормалізовані поля ... } ]
    }

Примітка: Форматовані рядкові значення (`price_str`, `volume_str`, `tp_sl`) додаються
щоб UI не перевизначав бізнес-логіку форматування.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Protocol

from rich.console import Console
from rich.logging import RichHandler

from config.config import REDIS_CHANNEL_ASSET_STATE, REDIS_SNAPSHOT_KEY
from utils.utils import format_price as fmt_price_stage1
from utils.utils import format_volume_usd

# ───────────────────────────── Логування ─────────────────────────────
logger = logging.getLogger("ui.publish_full_state")
if not logger.handlers:  # guard від повторної ініціалізації
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


class RedisLike(Protocol):
    async def publish(
        self, channel: str, message: str
    ) -> int:  # pragma: no cover - типізація
        ...

    async def set(self, key: str, value: str) -> object:  # pragma: no cover - типізація
        ...


class AssetStateManagerProto(Protocol):
    def get_all_assets(self) -> list[dict[str, Any]]:  # pragma: no cover - типізація
        ...


async def publish_full_state(
    state_manager: AssetStateManagerProto, cache_handler: object, redis_conn: RedisLike
) -> None:
    """Публікує агрегований стан активів у Redis одним повідомленням.

    Формат payload (type = REDIS_CHANNEL_ASSET_STATE):
        {
            "type": REDIS_CHANNEL_ASSET_STATE,
            "meta": {"ts": ISO8601UTC},
            "counters": {"assets": N, "alerts": A},
            "assets": [ ... нормалізовані поля ... ]
        }

    UI може брати заголовок зі ``counters``, а таблицю — з ``assets``.

    Args:
        state_manager: Постачальник станів активів (має метод ``get_all_assets()``).
        cache_handler: Резервний параметр для майбутнього кешу (не використовується).
        redis_conn: Підключення до Redis із методами ``publish`` та ``set``.

    Returns:
        None: Побічно публікує повідомлення у канал і зберігає снапшот у Redis.

    Raises:
        Винятки драйвера Redis або серіалізації зазвичай перехоплюються та логуються,
        оскільки виконання обгорнуто у блок ``try`` (best‑effort).
    """
    try:
        all_assets = state_manager.get_all_assets()  # список dict
        serialized_assets: list[dict[str, Any]] = []

        for asset in all_assets:
            # Захист: stats має бути dict
            if not isinstance(asset.get("stats"), dict):
                asset["stats"] = {}
            # числові поля для рядка таблиці
            for key in ["tp", "sl", "rsi", "volume", "atr", "confidence"]:
                if key in asset:
                    try:
                        asset[key] = (
                            float(asset[key])
                            if asset[key] not in [None, "", "NaN"]
                            else 0.0
                        )
                    except (TypeError, ValueError):
                        asset[key] = 0.0

            # ціна для UI: форматування виконується нижче через fmt_price_stage1

            # нормалізуємо базові статс (лише якщо ключ існує; не вводимо штучні 0.0)
            if "stats" in asset:
                for stat_key in [
                    "current_price",
                    "atr",
                    "volume_mean",
                    "open_interest",
                    "rsi",
                    "rel_strength",
                    "btc_dependency_score",
                ]:
                    if stat_key in asset["stats"]:
                        try:
                            val = asset["stats"][stat_key]
                            asset["stats"][stat_key] = (
                                float(val) if val not in [None, "", "NaN"] else None
                            )
                        except (TypeError, ValueError):  # narrow: очікувана валідація
                            asset["stats"][stat_key] = None

            # ── UI flattening layer ────────────────────────────────────────
            stats = asset.get("stats") or {}
            # Уніфіковані кореневі ключі, щоб UI не мав додаткових мапперів
            # Ціну виставляємо ТІЛЬКИ якщо вона валідна (>0); інакше не створюємо поля
            if "price" not in asset:
                cp = stats.get("current_price")
                try:
                    cp_f = float(cp) if cp is not None else None
                except Exception:
                    cp_f = None
                if cp_f is not None and cp_f > 0:
                    asset["price"] = cp_f
            # Форматовані рядкові версії (для UI без повторного форматування)
            if (
                "price_str" not in asset
                and isinstance(asset.get("price"), (int, float))
                and asset.get("price", 0) > 0
            ):
                try:
                    asset["price_str"] = fmt_price_stage1(
                        float(asset["price"]), str(asset.get("symbol", "")).lower()
                    )
                except Exception:  # broad except: форматування ціни не критичне
                    pass
            # Raw volume_mean (кількість контрактів/штук) → зберігаємо як raw_volume
            if "raw_volume" not in asset:
                vm = stats.get("volume_mean")
                if isinstance(vm, (int, float)):
                    asset["raw_volume"] = float(vm)
            # Обчислюємо оборот у USD (notional) = raw_volume * current_price
            if "volume" not in asset:
                cp_val = stats.get("current_price")
                try:
                    cp_f = float(cp_val) if cp_val is not None else None
                except Exception:
                    cp_f = None
                if (
                    isinstance(asset.get("raw_volume"), (int, float))
                    and cp_f is not None
                    and cp_f > 0
                ):
                    asset["volume"] = float(asset["raw_volume"]) * float(cp_f)
            if (
                "volume_str" not in asset
                and isinstance(asset.get("volume"), (int, float))
                and float(asset.get("volume") or 0) > 0
            ):
                try:
                    asset["volume_str"] = format_volume_usd(float(asset["volume"]))
                except Exception:  # broad except: форматування volume_str не критичне
                    pass
            # ATR% (для швидкого відтворення у UI без ділення щоразу)
            if "atr_pct" not in asset:
                atr_v = stats.get("atr")
                cp = stats.get("current_price")
                try:
                    atr_f = float(atr_v) if atr_v is not None else None
                except Exception:
                    atr_f = None
                try:
                    cp_f = float(cp) if cp is not None else None
                except Exception:
                    cp_f = None
                if atr_f is not None and cp_f is not None and cp_f > 0:
                    asset["atr_pct"] = float(atr_f) / float(cp_f) * 100.0
            # rsi додаємо лише якщо воно дійсно присутнє у stats і це число
            if "rsi" not in asset:
                rsi_v = stats.get("rsi")
                try:
                    rsi_f = float(rsi_v) if rsi_v is not None else None
                except Exception:
                    rsi_f = None
                if rsi_f is not None:
                    asset["rsi"] = rsi_f
            # status: перераховуємо щоразу, щоб не застрягав у 'init'
            status_val = asset.get("state")
            if isinstance(status_val, dict):  # захист
                status_val = status_val.get("status") or status_val.get("state")
            if not isinstance(status_val, str) or not status_val:
                status_val = (
                    asset.get("scenario") or asset.get("stage2_status") or "normal"
                )
            # Більше НЕ замінюємо 'init' на 'initializing' – коротка форма
            asset["status"] = status_val

            # tp_sl: формуємо завжди з поточних tp/sl (форматуючи ціну)
            tp = asset.get("tp")
            sl = asset.get("sl")
            tp_ok = isinstance(tp, (int, float)) and tp not in [None, 0]
            sl_ok = isinstance(sl, (int, float)) and sl not in [None, 0]
            try:
                sym = str(asset.get("symbol", "")).lower()
                fmt_tp = fmt_price_stage1(float(tp or 0.0), sym) if tp_ok else None
                fmt_sl = fmt_price_stage1(float(sl or 0.0), sym) if sl_ok else None
            except Exception:
                fmt_tp = str(tp) if tp_ok else None
                fmt_sl = str(sl) if sl_ok else None
            if tp_ok and sl_ok:
                asset["tp_sl"] = f"TP: {fmt_tp} | SL: {fmt_sl}"
            elif tp_ok:
                asset["tp_sl"] = f"TP: {fmt_tp}"
            elif sl_ok:
                asset["tp_sl"] = f"SL: {fmt_sl}"
            else:
                asset["tp_sl"] = "-"
            # гарантуємо signal (для UI фільтра)
            if not asset.get("signal"):
                asset["signal"] = "NONE"
            # видимість (fallback True якщо не задано)
            if "visible" in asset and asset["visible"] is False:
                pass  # залишаємо як є
            else:
                asset.setdefault("visible", True)

            # Проксі метаданих HTF для UI: витягуємо з market_context.meta
            try:
                mc = asset.get("market_context") or {}
                meta = mc.get("meta") if isinstance(mc, dict) else {}
                if isinstance(meta, dict):
                    if "htf_alignment" in meta and "htf_alignment" not in asset:
                        val = meta.get("htf_alignment")
                        if isinstance(val, (int, float)):
                            asset["htf_alignment"] = float(val)
                    if "htf_ok" in meta and "htf_ok" not in asset:
                        hov = meta.get("htf_ok")
                        if isinstance(hov, bool):
                            asset["htf_ok"] = hov
            except Exception:
                pass

            serialized_assets.append(asset)

        # counters для хедера
        counters = {
            "assets": len(serialized_assets),
            "alerts": len(
                [
                    a
                    for a in serialized_assets
                    if str(a.get("signal", "")).upper().startswith("ALERT")
                ]
            ),
        }

        # Нормалізуємо символи для UI (єдиний формат UPPER)
        for a in serialized_assets:
            if isinstance(a, dict) and "symbol" in a:
                try:
                    a["symbol"] = str(a["symbol"]).upper()
                except Exception:  # broad except: upper-case sanitation
                    pass

        payload = {
            "type": REDIS_CHANNEL_ASSET_STATE,
            "meta": {"ts": datetime.utcnow().isoformat() + "Z"},
            "counters": counters,
            "assets": serialized_assets,
        }

        try:
            if serialized_assets:
                first_keys = list(serialized_assets[0].keys())
            else:
                first_keys = []
            logger.debug(
                "Publish payload counters=%s assets_len=%d first_asset_keys=%s",
                counters,
                len(serialized_assets),
                first_keys,
            )
        except Exception:
            pass

        payload_json = json.dumps(payload, default=str)
        await redis_conn.publish(REDIS_CHANNEL_ASSET_STATE, payload_json)
        # Зберігаємо снапшот останнього повного стану (для швидкого старту UI)
        try:
            await redis_conn.set(REDIS_SNAPSHOT_KEY, payload_json)
        except Exception:  # broad except: snapshot optional
            logger.debug(
                "Не вдалося записати snapshot key=%s", REDIS_SNAPSHOT_KEY, exc_info=True
            )
        logger.info(f"✅ Опубліковано стан {len(serialized_assets)} активів")

    except Exception as e:  # broad except: публікація best-effort
        logger.error(f"Помилка публікації стану: {str(e)}")


__all__ = ["publish_full_state"]
# -*- coding: utf-8 -*-
