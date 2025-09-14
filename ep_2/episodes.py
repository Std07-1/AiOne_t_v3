# ep_2\episodes.py
# -*- coding: utf-8 -*-
"""
Автопошук «великих рухів» (епізодів) + зняття фіч + кластеризація + перевірка покриття сигналами.
"""

# Приклад запуску:

# python ep_2\episodes.py

from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple, Any
import numpy as np
import pandas as pd
import os
import json
import logging
from enum import Enum
from pathlib import Path
import time
import asyncio

from ep_2.config_episodes import EPISODE_CONFIG, EPISODE_CONFIG_1M, EPISODE_CONFIG_5M
from ep_2.indicators import (
    ensure_indicators,
    analyze_data_volatility,
    calculate_max_runup_drawdown,
    find_impulse_peak,
)
from ep_2.core import (
    Episode,
    Direction,
    EpisodeConfig,
    build_features_table,
    cluster_patterns,
    reference_ranges,
    event_hits,
    coverage_score_from_hits,
    analyze_episode_coverage,
)

try:  # валідація без залежностей візуалізації
    from ep_2.visualize_episodes import visualize_episodes
except Exception:  # noqa: BLE001

    def visualize_episodes(*args, **kwargs):  # type: ignore
        logger.warning("visualize_episodes пропущено (matplotlib не встановлено)")


# Новий єдиний шлях отримання даних (централізація через data.raw_data)
from data.raw_data import OptimizedDataFetcher  # type: ignore

# CacheHandler більше не використовується — нова логіка отримання даних без прямого кеш-хендлера
import aiohttp  # type: ignore

# ── Інтеграція з основною системою сигналів (Stage1) ──
from stage1.asset_monitoring import AssetMonitorStage1
from app.asset_state_manager import AssetStateManager  # type: ignore


# Параметри для генерації системних сигналів (можна винести в config_episodes)
SYSTEM_SIGNAL_PARAMS = {
    "lookback": 100,  # кількість барів для вікна аналізу
    "symbol": "BTCUSDT",
}


def generate_system_signals(
    df: pd.DataFrame,
    *,
    symbol: str,
    lookback: int = 100,
    log_every: int = 200,
) -> pd.DataFrame:
    """Генерує системні сигнали (Stage1) використовуючи наявний AssetMonitorStage1.

    Повертає DataFrame колонок: ts, price, side, type, reasons
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts", "price", "side", "type", "reasons"])

    # Нормалізація: переконаємось, що індекс UTC DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        df = df.copy()
        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    records: List[Dict[str, Any]] = []

    # Ініціалізація стану Stage1
    # Ініціалізуємо менеджер з мінімально необхідним списком активів
    state_manager = AssetStateManager(initial_assets=[symbol])
    # AssetMonitorStage1 не приймає 'symbol' у конструкторі в актуальній версії
    # Передаємо тільки state_manager (cache_handler=None для спрощеного офлайн середовища)
    monitor = AssetMonitorStage1(cache_handler=None, state_manager=state_manager)

    closes = df["close"].astype(float)
    total_iters = len(df) - lookback
    if total_iters <= 0:
        return pd.DataFrame(columns=["ts", "price", "side", "type", "reasons"])

    for i in range(lookback, len(df)):
        window = df.iloc[i - lookback : i]
        # emulate processing — в реальній системі monitor оновлюється стрімом
        # У production монітор оновлюється інкрементальним стрімом барів.
        # Для офлайн/батч режиму змоделюємо мінімальну логіку: оновимо статистику
        # і вручну перевіримо тригери через приватні/внутрішні методи якщо доступні.
        signal = None
        try:  # ensure statistics (імітуючи інкрементальний апдейт)
            # update_statistics очікує асинхронний виклик, але ми маємо синхронний цикл
            # тому викличемо через asyncio.run у тимчасовому loop.
            async def _upd():
                await monitor.update_statistics(symbol, window.copy())

            asyncio.run(_upd())
        except Exception:
            pass
        # Простий евристичний сигнал: якщо останній volume_z або RSI екстремальні
        try:
            stats = monitor.asset_stats.get(symbol) or {}
            rsi = stats.get("rsi")
            volz = stats.get("volume_z")
            trigger_reasons: List[str] = []
            if rsi is not None and rsi >= 80:
                trigger_reasons.append("RSI_OVERBOUGHT")
            if rsi is not None and rsi <= 20:
                trigger_reasons.append("RSI_OVERSOLD")
            if volz is not None and volz >= 3:
                trigger_reasons.append("VOLUME_SPIKE")
            if trigger_reasons:
                signal = {
                    "type": (
                        "ALERT_BUY"
                        if ("RSI_OVERSOLD" in trigger_reasons)
                        else "ALERT_SELL"
                    ),
                    "trigger_reasons": trigger_reasons,
                }
        except Exception:
            signal = None
        if signal:
            sig_type = signal.get("type") or signal.get("signal_type") or "generic"
            records.append(
                {
                    "ts": window.index[-1],
                    "price": float(window["close"].iloc[-1]),
                    "side": "BUY" if "up" in str(sig_type).lower() else "SELL",
                    "type": "SYSTEM_SIGNAL",
                    "reasons": signal.get("trigger_reasons")
                    or signal.get("reasons", []),
                }
            )
        iter_idx = i - lookback + 1
        if iter_idx % log_every == 0 or iter_idx == total_iters:
            logger.info(
                f"[SYSTEM_SIG/{symbol}] прогрес {iter_idx}/{total_iters} ({iter_idx/total_iters:.1%}), зібрано={len(records)}"
            )

    if not records:
        return pd.DataFrame(columns=["ts", "price", "side", "type", "reasons"])
    out_df = pd.DataFrame(records)
    return out_df


#############################################
#  ЄДИНИЙ LOADER ЧЕРЕЗ OptimizedDataFetcher  #
#############################################
_FETCHER_SINGLETON: Optional[OptimizedDataFetcher] = None
_FETCH_LOOP: Optional[asyncio.AbstractEventLoop] = None


def _get_fetch_loop() -> asyncio.AbstractEventLoop:
    """Глобальний loop для усіх викликів fetcher (не закриваємо до завершення процесу)."""
    global _FETCH_LOOP
    if _FETCH_LOOP is None:
        _FETCH_LOOP = asyncio.new_event_loop()
    return _FETCH_LOOP


def _get_fetcher() -> OptimizedDataFetcher:
    global _FETCHER_SINGLETON
    if _FETCHER_SINGLETON is None:
        loop = _get_fetch_loop()
        # Прив'язуємося до глобального loop
        try:
            asyncio.set_event_loop(loop)
        except Exception:
            pass
        session = aiohttp.ClientSession()
        _FETCHER_SINGLETON = OptimizedDataFetcher(session=session, compress_cache=True)
        # Закриття сесії при завершенні процесу
        try:
            import atexit

            def _close_session():  # pragma: no cover
                try:
                    if not session.closed:
                        loop = _get_fetch_loop()
                        if loop.is_running():
                            loop.create_task(session.close())
                        else:
                            loop.run_until_complete(session.close())
                except Exception:
                    pass

            atexit.register(_close_session)
        except Exception:
            pass
    return _FETCHER_SINGLETON


def fetch_bars_via_raw_data(symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
    """Синхронна обгортка над OptimizedDataFetcher.get_data(...).

    Викликається з синхронного коду (episodes.py) через asyncio.run.
    Повертає DataFrame з індексом часу (UTC) та колонками open/high/low/close/volume.
    """

    async def _runner():
        fetcher = _get_fetcher()
        df = await fetcher.get_data(
            symbol,
            timeframe,
            limit=limit,
            read_cache=True,
            write_cache=True,
        )
        return df

    # Використовуємо окремий тимчасовий event loop замість asyncio.run, щоб уникнути
    # помилки "Event loop is closed" при багаторазових послідовних викликах у цьому модулі.
    try:
        loop = _get_fetch_loop()
        try:
            asyncio.set_event_loop(loop)
        except Exception:
            pass
        df = loop.run_until_complete(_runner())
    except Exception as e:  # pragma: no cover
        logger.error("[DATA] Помилка під час виконання fetch_bars_via_raw_data: %s", e)
        df = None
    if df is None or df.empty:
        logger.error(
            "[DATA] Не вдалося отримати дані через OptimizedDataFetcher для %s %s",
            symbol,
            timeframe,
        )
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    if not isinstance(df.index, pd.DatetimeIndex):
        if "timestamp" in df.columns:
            df.index = pd.to_datetime(df["timestamp"], utc=True)
        else:
            df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
    df.index.name = "time"
    base_cols = ["open", "high", "low", "close", "volume"]
    extra_cols = [c for c in df.columns if c not in base_cols]
    df = df[base_cols + extra_cols]
    if limit and len(df) > limit:
        df = df.iloc[-limit:]
    return df


# Налаштування логування
try:
    from rich.console import Console  # type: ignore
    from rich.logging import RichHandler  # type: ignore

    _HAS_RICH = True
except ImportError:
    _HAS_RICH = False

logger = logging.getLogger("Episodes")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    if _HAS_RICH:
        _h = RichHandler(console=Console(stderr=True), show_path=False)  # type: ignore[arg-type]
    else:
        _h = logging.StreamHandler()
        _h.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
        )
    logger.addHandler(_h)
    logger.propagate = False


## Direction, Episode перенесені у ep_2.core


# утиліти
def _human_int(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def _fmt_pct(x: float, digits: int = 2) -> str:
    try:
        return f"{100 * float(x):.{digits}f}%"
    except Exception:
        return str(x)


def _log_params(logger, params: Dict[str, Any], title="Параметри пошуку"):
    # Додаємо всі ключові метрики та налаштування
    keys_order = [
        "symbol",
        "timeframe",
        "move_pct_up",
        "move_pct_down",
        "min_bars",
        "max_bars",
        "close_col",
        "ema_span",
        "impulse_k",
        "cluster_k",
        "pad_bars",
        "retrace_ratio",
        "require_retrace",
        "min_gap_bars",
        "merge_adjacent",
        "max_episodes",
        "adaptive_threshold",
        # Нові параметри
        "volume_z_threshold",
        "rsi_overbought",
        "rsi_oversold",
        "tp_mult",
        "sl_mult",
        "low_gate",
    ]
    pretty = {}
    for k in keys_order:
        if k in params:
            v = params[k]
            if isinstance(v, (float, int)) and (
                "move_pct" in k or k.endswith("_ratio")
            ):
                pretty[k] = _fmt_pct(float(v))
            else:
                pretty[k] = v
    # Додаємо всі інші, які не були у keys_order
    for k, v in params.items():
        if k not in pretty:
            pretty[k] = v
    lines = [f"{title}"]
    for k, v in pretty.items():
        lines.append(f"  {k:18s}: {v}")
    logger.info("\n".join(lines))


# --- Єдина функція для красивого логування результатів аналізу ---
def log_analysis_results(results, logger):
    """
    Логування всіх ключових результатів аналізу у вертикальному форматі.
    """

    def log_block(lines, logger, title=None):
        border = "═" * 60
        pad = " " * 4
        out = []
        if title:
            out.append(f"\n{pad}{border}\n{pad}{title}")
        else:
            out.append(f"\n{pad}{border}")
        for line in lines:
            out.append(f"{pad}{line}")
        out.append(f"{pad}{border}\n")
        logger.info("\n".join(out))

    def log_dict_vertical(d, logger, title, float_digits=4, float_keys=None):
        # Українські назви для метрик
        ukr_names = {
            # Статистика покриття
            "coverage": "Покриття",
            "avg_abs_offset": "Середнє відхилення",
            "misses": "Пропущено епізодів",
            "total": "Всього епізодів",
            # Статистика сигналів
            "total_signals": "Всього сигналів",
            "signals_per_bar": "Сигналів на бар",
            "signals_in_episodes": "Сигналів в епізодах",
            "signals_near_episodes": "Сигналів біля епізодів",
        }
        lines = []
        for k, v in d.items():
            name = ukr_names.get(k, k)
            if float_keys is not None and k in float_keys and isinstance(v, float):
                v = f"{v:.{float_digits}f}"
            elif isinstance(v, float):
                v = f"{v:.{float_digits}f}"
            lines.append(f"{name:24s}: {v}")
        log_block(lines, logger, title)

    # Статистика сигналів
    if "signal_stats" in results:
        log_dict_vertical(
            results["signal_stats"],
            logger,
            "Статистика сигналів",
            float_digits=4,
            float_keys=["signals_per_bar"],
        )

    # Статистика покриття
    log_dict_vertical(results["stats"], logger, "Статистика покриття", float_digits=4)

    # Аналіз волатильності (3 знаки після крапки для float)
    log_dict_vertical(
        results["volatility_analysis"],
        logger,
        "Аналіз волатильності",
        float_digits=3,
        float_keys=[
            "max_5bar_return",
            "max_30bar_return",
            "max_60bar_return",
            "max_5bar_move",
            "max_30bar_move",
            "max_60bar_move",
        ],
    )

    # Preview features
    def log_features_vertical(df, logger, title="Preview features"):
        if df.empty:
            log_block([f"{title}: (empty)"], logger)
            return
        preview = df.head(3)
        for idx, row in preview.iterrows():
            lines = [f"{title} (row {idx}):"]
            for k, v in row.items():
                if k in ("t_start", "t_end") and isinstance(v, str):
                    v = v.split(".")[0] if "." in v else v
                elif k in ("t_start", "t_end") and hasattr(v, "isoformat"):
                    v = str(v)
                    v = v.split(".")[0] if "." in v else v
                elif isinstance(v, float):
                    v = f"{v:.4f}"
                lines.append(f"{k:18s}: {v}")
            log_block(lines, logger)

    if "features" in results and not results["features"].empty:
        log_features_vertical(results["features"], logger, title="Preview features")
    log_features_vertical(results["features"], logger, title="Preview features")


def find_episodes_v2(
    df: pd.DataFrame,
    move_pct_up: float = 0.02,
    move_pct_down: float = 0.02,
    min_bars: int = 20,
    max_bars: int = 750,
    close_col: str = "close",
    retrace_ratio: float = 0.33,
    require_retrace: bool = True,
    min_gap_bars: int = 0,
    merge_adjacent: bool = True,
    max_episodes: Optional[int] = None,
    adaptive_threshold: bool = False,
) -> List[Episode]:
    """
    Покращений пошук епізодів (plain logs + контроль щільності).
    """
    # ---- Параметри ----
    # Формуємо всі параметри для логування
    params_log = {
        "symbol": EPISODE_CONFIG.get("symbol", None),
        "timeframe": EPISODE_CONFIG.get("timeframe", None),
        "move_pct_up": move_pct_up,
        "move_pct_down": move_pct_down,
        "min_bars": min_bars,
        "max_bars": max_bars,
        "close_col": close_col,
        "ema_span": EPISODE_CONFIG.get("ema_span", None),
        "impulse_k": EPISODE_CONFIG.get("impulse_k", None),
        "cluster_k": EPISODE_CONFIG.get("cluster_k", None),
        "pad_bars": EPISODE_CONFIG.get("pad_bars", None),
        "retrace_ratio": retrace_ratio,
        "require_retrace": require_retrace,
        "min_gap_bars": min_gap_bars,
        "merge_adjacent": merge_adjacent,
        "max_episodes": max_episodes if max_episodes is not None else "None",
        "adaptive_threshold": adaptive_threshold,
    }
    _log_params(
        logger,
        params_log,
        title="Параметри пошуку епізодів: \n",
    )

    # ...видалено статистику колонок...

    # NaN діагностика
    if df.isna().any().any():
        nan_map = {c: int(df[c].isna().sum()) for c in df.columns if df[c].isna().any()}
        logger.warning(
            "NaN у даних:\n"
            + "\n".join([f"  {k}: {_human_int(v)}" for k, v in nan_map.items()])
        )

    # ---- Перевірки та підготовка ----
    assert close_col in df.columns, f"'{close_col}' column is required"
    n = len(df)
    if n == 0:
        logger.info("Порожній DF → епізодів: 0")
        return []

    close = df[close_col].astype(float).values

    logger.info("Запуск пошуку епізодів \n")

    # ---- Основний цикл пошуку  ----
    i = 0
    max_iterations = n
    iteration = 0
    episodes: List[Episode] = []

    while i < n - 1 and iteration < max_iterations:
        logger.debug(
            f"[find_episodes_v2] Крок: i={i}, ітерація={iteration}, базова ціна={close[i]:.4f}"
        )
        iteration += 1
        start = i
        base_price = close[start]
        local_min = base_price
        local_max = base_price
        local_min_idx = start
        local_max_idx = start
        direction: Optional[Direction] = None
        end = start
        episode_created = False

        for j in range(start + 1, min(n, start + max_bars + 1)):
            logger.debug(
                f"[find_episodes_v2] Внутрішній цикл: j={j}, ціна={close[j]:.4f}"
            )
            end = j
            p = close[j]

            # Оновлюємо локальні екстремуми
            if p < local_min:
                local_min = p
                local_min_idx = j
            if p > local_max:
                local_max = p
                local_max_idx = j

            # Перевіряємо умови для визначення напрямку
            up_move = (p - local_min) / max(local_min, 1e-12)
            down_move = (local_max - p) / max(local_max, 1e-12)

            logger.debug(
                f"[find_episodes_v2] up_move={up_move:.4f}, down_move={down_move:.4f}, direction={direction}"
            )
            if direction is None:
                if up_move >= move_pct_up:
                    direction = Direction.UP
                    logger.debug(
                        f"[find_episodes_v2] Виявлено рух вгору: індекс={j}, up_move={up_move:.4f}"
                    )
                elif down_move >= move_pct_down:
                    direction = Direction.DOWN
                    logger.debug(
                        f"[find_episodes_v2] Виявлено рух вниз: індекс={j}, down_move={down_move:.4f}"
                    )

            # Якщо напрямок визначено, перевіряємо умови завершення
            if direction is not None:
                logger.debug(
                    f"[find_episodes_v2] Перевірка retrace: direction={direction}, j={j}"
                )
                if direction == Direction.UP:
                    peak_price = close[local_max_idx]
                    retrace = (peak_price - p) / max(peak_price, 1e-12)

                    # Умова завершення: відскок або досягнення максимуму барів
                    logger.debug(
                        f"[find_episodes_v2] retrace для UP: retrace={retrace:.4f}, поріг retrace={retrace_ratio * move_pct_up:.4f}"
                    )
                    if (
                        (not require_retrace)
                        or (retrace >= retrace_ratio * move_pct_up)
                        or (j == min(n - 1, start + max_bars))
                    ):
                        ep_start = local_min_idx
                        ep_end = local_max_idx

                        if ep_end - ep_start + 1 >= min_bars:
                            logger.debug(
                                f"[find_episodes_v2] Довжина UP епізоду: {ep_end-ep_start+1}"
                            )
                            seg = close[ep_start : ep_end + 1]
                            move = (close[ep_end] - close[ep_start]) / max(
                                close[ep_start], 1e-12
                            )
                            runup, dd = calculate_max_runup_drawdown(
                                pd.Series(seg), Direction.UP.value
                            )

                            episodes.append(
                                Episode(
                                    start_idx=ep_start,
                                    end_idx=ep_end,
                                    direction=Direction.UP,
                                    peak_idx=local_max_idx,
                                    move_pct=float(move),
                                    duration_bars=int(ep_end - ep_start + 1),
                                    max_drawdown_pct=float(dd),
                                    max_runup_pct=float(runup),
                                    t_start=pd.Timestamp(df.index[ep_start]),
                                    t_end=pd.Timestamp(df.index[ep_end]),
                                )
                            )
                            episode_created = True
                            logger.debug(
                                f"[find_episodes_v2] Додано UP епізод: початок={ep_start}, кінець={ep_end}, рух={move:.4f}"
                            )
                        else:
                            logger.debug(
                                f"[find_episodes_v2] UP епізод занадто короткий: довжина={ep_end-ep_start+1}"
                            )

                        i = max(ep_end, j)
                        logger.debug(
                            f"[find_episodes_v2] Перехід до наступного після UP епізоду, i={i}"
                        )
                        break

                else:  # direction == Direction.DOWN
                    trough_price = close[local_min_idx]
                    retrace = (p - trough_price) / max(trough_price, 1e-12)

                    logger.debug(
                        f"[find_episodes_v2] retrace для DOWN: retrace={retrace:.4f}, поріг retrace={retrace_ratio * move_pct_down:.4f}"
                    )
                    if (
                        (not require_retrace)
                        or (retrace >= retrace_ratio * move_pct_down)
                        or (j == min(n - 1, start + max_bars))
                    ):
                        ep_start = local_max_idx
                        ep_end = local_min_idx

                        if ep_end - ep_start + 1 >= min_bars:
                            logger.debug(
                                f"[find_episodes_v2] Довжина DOWN епізоду: {ep_end-ep_start+1}"
                            )
                            seg = close[ep_start : ep_end + 1]
                            move = (close[ep_end] - close[ep_start]) / max(
                                close[ep_start], 1e-12
                            )
                            runup, dd = calculate_max_runup_drawdown(
                                pd.Series(seg), Direction.DOWN.value
                            )

                            episodes.append(
                                Episode(
                                    start_idx=ep_start,
                                    end_idx=ep_end,
                                    direction=Direction.DOWN,
                                    peak_idx=local_min_idx,
                                    move_pct=float(move),
                                    duration_bars=int(ep_end - ep_start + 1),
                                    max_drawdown_pct=float(dd),
                                    max_runup_pct=float(runup),
                                    t_start=pd.Timestamp(df.index[ep_start]),
                                    t_end=pd.Timestamp(df.index[ep_end]),
                                )
                            )
                            episode_created = True
                            logger.debug(
                                f"[find_episodes_v2] Додано DOWN епізод: початок={ep_start}, кінець={ep_end}, рух={move:.4f}"
                            )
                        else:
                            logger.debug(
                                f"[find_episodes_v2] DOWN епізод занадто короткий: довжина={ep_end-ep_start+1}"
                            )

                        i = max(ep_end, j)
                        logger.debug(
                            f"[find_episodes_v2] Перехід до наступного після DOWN епізоду, i={i}"
                        )
                        break

        # Якщо епізод не був створений, переходимо до наступного бару
        if not episode_created:
            i += 1
            logger.debug(
                f"[find_episodes_v2] Продовжую, епізод не знайдено, наступний i={i}"
            )

        # Якщо епізод не був створений, переходимо до наступного бару
        if not episode_created:
            i += 1
            logger.debug(
                f"[find_episodes_v2] Продовжую, епізод не знайдено, наступний i={i}"
            )

    # Фільтруємо епізоди за мінімальною довжиною
    logger.debug(f"[find_episodes_v2] Основний цикл завершено, ітерацій={iteration}")
    episodes_before_filter = len(episodes)
    episodes = [ep for ep in episodes if ep.duration_bars >= min_bars]
    if episodes_before_filter > 0 and len(episodes) == 0:
        logger.warning(
            f"[find_episodes_v2] Всі епізоди відфільтровані як занадто короткі (min_bars={min_bars})"
        )
    episodes.sort(key=lambda e: e.start_idx)
    if len(episodes) == 0:
        logger.info(
            f"[find_episodes_v2] Не знайдено жодного епізоду"
        )  # вже українською
    else:
        logger.debug(f"[find_episodes_v2] Завершено, знайдено {len(episodes)} епізодів")

    return episodes


# Додамо наступні функції та модифікації
## Генерація тестових сигналів (create_sample_signals) та early_burst вилучено — використовуються лише системні Stage1 сигнали


## analyze_episode_coverage тепер імпортується з ep_2.core


## episode_features / build_features_table тепер у ep_2.core


# КОНФІГУРАЦІЯ
## EpisodeConfig тепер імпортується з ep_2.core


# ОСНОВНА ФУНКЦІЯ
def main_analysis(
    df: pd.DataFrame,
    config: EpisodeConfig,
    entry_mask: Optional[pd.Series] = None,
    create_sample: bool = False,
) -> Dict[str, Any]:
    # 1) Індикатори (RSI/ATR/atr_pct/volume_z)
    df = ensure_indicators(df)

    # (Вилучено) Генерація штучних сигналів — тепер працюємо виключно з системними сигналами Stage1

    # 2) Аналіз волатильності
    volatility = analyze_data_volatility(df)

    # 3) Автопідбір порогів за волатильністю (якщо задано <= 0)
    if config.move_pct_up <= 0:
        max_60_move = float(volatility.get("max_60bar_move", 0.01) or 0.01)
        config.move_pct_up = max(0.005, min(0.02, max_60_move * 0.5))
        config.move_pct_down = config.move_pct_up
        logger.info(
            "Автоматично встановлено move_pct_up=%.4f (down=%.4f) на основі волатильності",
            config.move_pct_up,
            config.move_pct_down,
        )

    # 4) Безпечне зняття додаткових параметрів (можуть бути відсутні у старому EpisodeConfig)
    min_gap_bars = getattr(config, "min_gap_bars", 0)
    merge_adjacent = getattr(config, "merge_adjacent", True)
    max_episodes = getattr(config, "max_episodes", None)
    adaptive_threshold = getattr(config, "adaptive_threshold", False)

    # 5) Пошук епізодів
    episodes = find_episodes_v2(
        df,
        move_pct_up=config.move_pct_up,
        move_pct_down=config.move_pct_down,
        min_bars=config.min_bars,
        max_bars=config.max_bars,
        close_col=config.close_col,
        retrace_ratio=config.retrace_ratio,
        require_retrace=config.require_retrace,
        # нові контролі щільності:
        min_gap_bars=min_gap_bars,
        merge_adjacent=merge_adjacent,
        max_episodes=max_episodes,
        adaptive_threshold=adaptive_threshold,
    )

    # 6) Побудова характеристик епізодів
    features_df = build_features_table(
        df, episodes, ema_span=config.ema_span, impulse_k=config.impulse_k
    )

    # Кластеризація патернів
    if not features_df.empty:
        features_df["cluster"] = cluster_patterns(features_df, k=config.cluster_k)
        ref_ranges = reference_ranges(features_df)
    else:
        ref_ranges = {}

    # 7) Покриття сигналами
    if entry_mask is None:
        entry_mask = pd.Series(False, index=df.index, dtype=bool)
    else:
        # гарантуємо правильний індекс/тип
        entry_mask = entry_mask.reindex(df.index).fillna(False).astype(bool)

    hits_df, stats = event_hits(
        entry_mask, episodes, pad=getattr(config, "pad_bars", 5)
    )
    score = coverage_score_from_hits(stats)

    # Детальний аналіз покриття
    coverage_details = analyze_episode_coverage(
        episodes, entry_mask, df, pad=config.pad_bars
    )

    # Аналіз ефективності сигналів
    if entry_mask.any():
        if not coverage_details.empty and "signals_during" in coverage_details.columns:
            signals_in_episodes = int(coverage_details["signals_during"].sum())
        else:
            signals_in_episodes = 0
        if (
            not coverage_details.empty
            and "signals_in_window" in coverage_details.columns
        ):
            signals_near_episodes = int(coverage_details["signals_in_window"].sum())
        else:
            signals_near_episodes = 0
        signal_stats = {
            "total_signals": int(entry_mask.sum()),
            "signals_per_bar": float(entry_mask.mean()),
            "signals_in_episodes": signals_in_episodes,
            "signals_near_episodes": signals_near_episodes,
        }

        def log_dict_vertical(d, logger, title, float_digits=4, float_keys=None):
            lines = [f"{title}:"]
            for k, v in d.items():
                if float_keys is not None and k in float_keys and isinstance(v, float):
                    v = f"{v:.{float_digits}f}"
                elif isinstance(v, float):
                    v = f"{v:.{float_digits}f}"
                lines.append(f"  {k:18s}: {v}")
            logger.info("\n".join(lines))

        log_dict_vertical(
            signal_stats,
            logger,
            "Статистика сигналів",
            float_digits=4,
            float_keys=["signals_per_bar"],
        )  # вертикальний формат

    # 8) Вихід
    return {
        "episodes": episodes,
        "features": features_df,
        "reference_ranges": ref_ranges,
        "hits": hits_df,
        "stats": stats,
        "coverage_score": score,
        "volatility_analysis": volatility,
        "coverage_details": coverage_details,
        "entry_mask": entry_mask,
    }


# УТІЛІТИ
def read_csv_smart(path: str) -> pd.DataFrame:
    time_cols = ["timestamp", "open_time", "time", "datetime", "date"]
    df = pd.read_csv(path)

    ts_col = None
    for c in time_cols:
        if c in df.columns:
            ts_col = c
            break

    if ts_col is None:
        ts_col = df.columns[0]

    df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    df = df.dropna(subset=[ts_col]).set_index(ts_col).sort_index()

    return df


# --- УНІВЕРСАЛЬНЕ ЗАВАНТАЖЕННЯ ДАНИХ (Hierarchical Loader) ---
def _read_datastore_snapshot(
    symbol: str, timeframe: str, base_dir: str = "datastore"
) -> Optional[pd.DataFrame]:
    """Пробує прочитати snapshot з datastore/, який створює UnifiedDataStore.

    Формат файлу:  {base_dir}/{symbol}_bars_{timeframe}_snapshot.jsonl
    Кожен рядок – JSON-об'єкт із обов'язковими колонками open_time, open, high, low, close, volume.
    """
    path = Path(base_dir) / f"{symbol}_bars_{timeframe}_snapshot.jsonl"
    if not path.exists():
        return None
    try:
        rows = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        if not rows:
            return None
        df = pd.DataFrame(rows)
        # Очікуваний timestamp стовпець: open_time (ms)
        if "open_time" in df.columns:
            # підтримка як ms (int) так і ISO
            if pd.api.types.is_integer_dtype(df["open_time"]):
                df["open_time"] = pd.to_datetime(
                    df["open_time"], unit="ms", utc=True, errors="coerce"
                )
            else:
                df["open_time"] = pd.to_datetime(
                    df["open_time"], utc=True, errors="coerce"
                )
            df = df.dropna(subset=["open_time"]).set_index("open_time").sort_index()
        elif "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
            df = df.dropna(subset=["time"]).set_index("time").sort_index()
        # Стандартизуємо назви
        rename_map = {"Time": "time", "Volume": "volume", "Close": "close"}
        for k, v in rename_map.items():
            if k in df.columns and v not in df.columns:
                df.rename(columns={k: v}, inplace=True)
        return df
    except Exception as e:
        logger.warning(f"Не вдалося прочитати datastore snapshot: {e}")
        return None


def _scan_csv_variants(symbol: str, timeframe: str) -> List[Path]:
    """Повертає список CSV файлів формату bars_{symbol}_{timeframe}_*.csv (відсортований за числовим * у спадному порядку)."""
    pattern = f"bars_{symbol}_{timeframe}_*.csv"
    files = list(Path(".").glob(pattern))

    def _extract_limit(p: Path) -> int:
        try:
            # bars_SYMBOL_TF_LIMIT.csv -> беремо останній компонент перед .csv
            return int(p.stem.split("_")[-1])
        except Exception:
            return -1

    files.sort(key=_extract_limit, reverse=True)
    return files


def load_bars_auto(config: "EpisodeConfig") -> pd.DataFrame:
    """Спрощений loader: завжди отримує дані через OptimizedDataFetcher (raw_data).

    Видалено підтримку snapshot/CSV та параметр force_live. Єдиним джерелом є кеш + REST.
    """
    symbol = config.symbol
    timeframe = config.timeframe
    limit = getattr(config, "limit", 0) or 0
    logger.info(
        "[DATA] Завантаження через OptimizedDataFetcher (single-source) %s %s limit=%s",
        symbol,
        timeframe,
        limit,
    )
    df = fetch_bars_via_raw_data(symbol, timeframe, limit)
    if df.empty:
        raise RuntimeError(
            f"Не вдалося отримати дані для {symbol} {timeframe} (single-source raw_data)"
        )
    return df


## cluster_patterns / reference_ranges / event_hits / coverage_score_from_hits тепер у ep_2.core


if __name__ == "__main__":
    start_time = time.time()

    # Завантажуємо конфігурації для двох таймфреймів
    configs = {
        "1m": EpisodeConfig(**EPISODE_CONFIG_1M),
        "5m": EpisodeConfig(**EPISODE_CONFIG_5M),
    }

    all_results = {}

    # Завантажуємо конфігурацію з файлу
    # config = EpisodeConfig(**EPISODE_CONFIG)

    # Формуємо шлях до CSV файлу на основі конфігурації
    # csv_path = f"bars_{config.symbol}_{config.timeframe}_{config.limit}.csv"

    for tf, config in configs.items():
        logger.info(f"Аналіз для таймфрейму {tf}")
        # Використовуємо ієрархічний loader замість жорсткого CSV
        df = load_bars_auto(config)
        logger.debug(
            f"Розмір DataFrame після auto-load: {df.shape}, колонки: {list(df.columns)}"
        )

        if df.empty:
            logger.error("Дані не прочитані або порожні!")
            raise SystemExit(1)

        logger.info(f"Аналіз символу: {config.symbol}")

        # Генерація системних сигналів (Stage1) ДО основного аналізу для інтеграції в coverage
        system_signals_df = None
        try:
            system_signals_df = generate_system_signals(
                df,
                symbol=config.symbol,
                lookback=SYSTEM_SIGNAL_PARAMS.get("lookback", 100),
            )
            if system_signals_df is not None and not system_signals_df.empty:
                logger.info(
                    f"Інтегровано {len(system_signals_df)} системних сигналів (Stage1) для {tf} у coverage"
                )
        except Exception as e:
            logger.warning(f"Не вдалося згенерувати системні сигнали: {e}")
            system_signals_df = None

        # Формуємо entry_mask з системних сигналів (true на таймстемпах сигналів)
        entry_mask = None
        if system_signals_df is not None and not system_signals_df.empty:
            # нормалізувати індекси до наявних у df (nearest exact match)
            ts_set = set(system_signals_df["ts"].astype("datetime64[ns, UTC]").values)
            entry_mask = df.index.isin(ts_set)
            entry_mask = pd.Series(entry_mask, index=df.index, dtype=bool)

        # Запуск аналізу з побудованим entry_mask
        results = main_analysis(
            df,
            config,
            entry_mask=entry_mask,
            create_sample=False,
        )
        # Додаємо системні сигнали у результати для подальшого використання/збереження
        results["system_signals"] = system_signals_df
        all_results[tf] = results

        # Логування результатів
        logger.info(f"Результати для {tf}:")

        # Додатковий аналіз покриття
        coverage_details = results["coverage_details"]
        if not coverage_details.empty and "covered" in coverage_details.columns:
            coverage_rate = coverage_details["covered"].mean()
        else:
            coverage_rate = 0.0

        logger.info(f"Знайдено {len(results['episodes'])} епізодів")  # вже українською
        logger.info(f"Відсоток покритих епізодів: {coverage_rate:.2%}")
        logger.info(f"Оцінка покриття: {results['coverage_score']:.3f}")

        def log_dict_vertical(d, logger, title, float_digits=4, float_keys=None):
            lines = [f"{title}:"]
            for k, v in d.items():
                if float_keys is not None and k in float_keys and isinstance(v, float):
                    v = f"{v:.{float_digits}f}"
                elif isinstance(v, float):
                    v = f"{v:.{float_digits}f}"
                lines.append(f"  {k:18s}: {v}")
            logger.info("\n".join(lines))

        # Статистика покриття
        log_dict_vertical(
            results["stats"], logger, "Статистика покриття", float_digits=4
        )
        # Аналіз волатильності (3 знаки після крапки для float)
        log_dict_vertical(
            results["volatility_analysis"],
            logger,
            "Аналіз волатильності",
            float_digits=3,
            float_keys=[
                "max_5bar_return",
                "max_30bar_return",
                "max_60bar_return",
                "max_5bar_move",
                "max_30bar_move",
                "max_60bar_move",
            ],
        )

        def log_features_vertical(df, logger, title="Preview features"):
            if df.empty:
                logger.info(f"{title}: (empty)")
                return
            preview = df.head(3)
            for idx, row in preview.iterrows():
                lines = [f"{title} (row {idx}):"]
                for k, v in row.items():
                    # Обрізаємо t_start/t_end
                    if k in ("t_start", "t_end") and isinstance(v, str):
                        # Прибираємо .999000+00:00
                        v = v.split(".")[0] if "." in v else v
                    elif k in ("t_start", "t_end") and hasattr(v, "isoformat"):
                        v = str(v)
                        v = v.split(".")[0] if "." in v else v
                    # Обрізаємо числові значення
                    elif isinstance(v, float):
                        v = f"{v:.4f}"
                    lines.append(f"  {k:18s}: {v}")
                logger.info("\n".join(lines))

        if not results["features"].empty:
            log_features_vertical(results["features"], logger, title="Preview features")

        # Візуалізація з TP/SL зонами, PnL та інтегрованими системними сигналами
        try:
            Path("results").mkdir(exist_ok=True)
            out_file = f"results/{config.symbol}_{tf}_episodes.pdf"
            merged_signals = results.get("system_signals")

            visualize_episodes(
                df,
                results["episodes"],
                signals_df=merged_signals,  # локальні сигнали, прикріплені до епізодів
                global_signals_df=merged_signals,  # повний список для глобального шару
                save_path=out_file,
                show_signals=True,
                show_anchors=True,
                price_col="close",
                annotate_metrics=True,
                show_tp_sl_zones=True,
                label_trade_pnl=True,
                tp_pct=(
                    0.02
                    if getattr(config, "default_tp_pct", None) is None
                    else config.default_tp_pct
                ),
                sl_pct=(
                    0.012
                    if getattr(config, "default_sl_pct", None) is None
                    else config.default_sl_pct
                ),
                # --- нові параметри глобальних сигналів / покриття ---
                show_global_signals=True,
                global_coverage_warn_ratio=0.25,  # попередження якщо <25% всередині
                inside_buffer_minutes=0,  # можна підняти для «м'якого» вікна
                highlight_edge_minutes=5,
                outside_mode="all",  # 'edge-only' або 'none' для фільтрації
            )
            logger.info(f"Збережено візуалізацію: {out_file}")
        except ImportError:
            logger.warning("Пропущено візуалізацію (потрібен matplotlib)")

        # Збереження результатів
        if config.save_results:
            prefix = f"episodes_{config.symbol}_{config.timeframe}_{config.limit}"
            Path("results").mkdir(exist_ok=True)

            ep_out = f"results/{prefix}_episodes.jsonl"
            with open(ep_out, "w", encoding="utf-8") as f:
                for e in results["episodes"]:
                    f.write(
                        json.dumps(e.to_dict(), ensure_ascii=False, default=str) + "\n"
                    )

            ft_out = f"results/{prefix}_features.csv"
            results["features"].fillna("").to_csv(ft_out, index=False)

            rr_out = f"results/{prefix}_ref_ranges.json"
            with open(rr_out, "w", encoding="utf-8") as f:
                json.dump(
                    results["reference_ranges"],
                    f,
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )

            ht_out = f"results/{prefix}_hits.csv"
            results["hits"].fillna("").to_csv(ht_out, index=False)

            # Зберігаємо системні сигнали (якщо є) для прозорості
            if results.get("system_signals") is not None and not results["system_signals"].empty:  # type: ignore[attr-defined]
                sys_out = f"results/{prefix}_system_signals.csv"
                results["system_signals"].to_csv(sys_out, index=False)  # type: ignore[index]
            else:
                sys_out = None

            config_out = f"results/{prefix}_config.json"
            with open(config_out, "w", encoding="utf-8") as f:
                json.dump(config.to_dict(), f, indent=2)

            extra_line = f"\n  {sys_out}" if sys_out else ""
            logger.info(
                f"Результати збережено у теці results:\n  {ep_out}\n  {ft_out}\n  {rr_out}\n  {ht_out}\n  {config_out}{extra_line}"
            )

    end_time = time.time()
    logger.info(f"Аналіз завершено за {end_time - start_time:.2f} секунд")
