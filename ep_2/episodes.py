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

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from config_episodes import EPISODE_CONFIG, EPISODE_CONFIG_1M, EPISODE_CONFIG_5M
from indicators import (
    ensure_indicators,
    analyze_data_volatility,
    create_early_burst_signals,
    calculate_atr_percent,
    calculate_vwap,
    calculate_ema,
    calculate_linreg_slope,
    segment_data,
    calculate_max_runup_drawdown,
    find_impulse_peak,
)
from visualize_episodes import visualize_episodes

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


class Direction(str, Enum):
    UP = "up"
    DOWN = "down"


# МОДЕЛІ ДАНИХ
@dataclass
class Episode:
    """Відрізок потужного руху ціни."""

    start_idx: int
    end_idx: int
    direction: Direction
    peak_idx: int
    move_pct: float
    duration_bars: int
    max_drawdown_pct: float
    max_runup_pct: float
    t_start: pd.Timestamp
    t_end: pd.Timestamp

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["t_start"] = self.t_start.isoformat()
        d["t_end"] = self.t_end.isoformat()
        d["direction"] = self.direction.value
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Episode:
        """Створює Episode з словника."""
        data = data.copy()
        data["direction"] = Direction(data["direction"])
        data["t_start"] = pd.Timestamp(data["t_start"])
        data["t_end"] = pd.Timestamp(data["t_end"])
        return cls(**data)


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
        "signal_type",
        "early_burst_pad",
        "weights",
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
def create_sample_signals(df: pd.DataFrame, signal_prob: float = 0.1) -> pd.Series:
    """
    Створює випадкові сигнали для тестування покриття епізодів.

    Args:
        df: DataFrame з даними
        signal_prob: Ймовірність сигналу в кожному барі

    Returns:
        Серія з булевими сигналами
    """
    np.random.seed(42)  # Для відтворюваності
    signals = np.random.random(len(df)) < signal_prob
    return pd.Series(signals, index=df.index, name="entry_signal")


def analyze_episode_coverage(
    episodes: List[Episode], entry_mask: pd.Series, df: pd.DataFrame, pad: int = 5
) -> pd.DataFrame:
    """
    Детальний аналіз покриття епізодів сигналами.

    Args:
        episodes: Список епізодів
        entry_mask: Маска сигналів входу
        df: DataFrame з даними
        pad: Кількість барів для розширення вікна пошуку

    Returns:
        DataFrame з детальною статистикою покриття
    """
    if not episodes:
        return pd.DataFrame()

    results = []

    for i, ep in enumerate(episodes):
        # Знаходимо пік імпульсу
        segment = df.iloc[ep.start_idx : ep.end_idx + 1]
        peak_time = find_impulse_peak(segment, ep.direction.value)

        # Вікно пошуку навколо піку
        peak_idx = df.index.get_loc(peak_time)
        start_search = max(0, peak_idx - pad)
        end_search = min(len(df) - 1, peak_idx + pad)

        # Вибірка сигналів у вікні
        window_signals = entry_mask.iloc[start_search : end_search + 1]
        signal_indices = window_signals[window_signals].index.tolist()

        # Перший сигнал та його зміщення від піку
        first_signal_idx = None
        first_signal_offset = None
        if signal_indices:
            first_signal_idx = signal_indices[0]
            first_signal_offset = entry_mask.index.get_loc(first_signal_idx) - peak_idx

        # Підрахунок сигналів до, під час та після епізоду
        signals_before = entry_mask.iloc[:start_search].sum()
        signals_during = entry_mask.iloc[start_search : end_search + 1].sum()
        signals_after = entry_mask.iloc[end_search + 1 :].sum()

        results.append(
            {
                "episode_id": i,
                "direction": ep.direction.value,
                "move_pct": ep.move_pct,
                "duration_bars": ep.duration_bars,
                "signals_in_window": len(signal_indices),
                "first_signal_offset": first_signal_offset,
                "signals_before": signals_before,
                "signals_during": signals_during,
                "signals_after": signals_after,
                "covered": len(signal_indices) > 0,
            }
        )

    return pd.DataFrame(results)


# ФІЧІ НА ЕПІЗОД
def episode_features(
    df: pd.DataFrame,
    ep: Episode,
    ema_span: int = 50,
    impulse_k: int = 8,
) -> Dict[str, Any]:
    seg = segment_data(df, ep.start_idx, ep.end_idx).copy()
    seg_len = len(seg)

    if seg_len == 0:
        return {}

    def safe_get(series: pd.Series, idx: pd.Timestamp, default: float = 0.0) -> float:
        try:
            return float(series.loc[idx])
        except (KeyError, ValueError):
            return default

    atrp = calculate_atr_percent(seg)
    rsi = (
        seg["rsi"]
        if "rsi" in seg.columns
        else pd.Series([0.0] * seg_len, index=seg.index)
    )
    volz = (
        seg["volume_z"]
        if "volume_z" in seg.columns
        else pd.Series([0.0] * seg_len, index=seg.index)
    )
    close = seg["close"].astype(float)

    s_idx = seg.index[0]
    e_idx = seg.index[-1]
    p_idx = df.index[ep.peak_idx]

    rsi_s = safe_get(rsi, s_idx)
    rsi_p = safe_get(rsi, p_idx)
    rsi_e = safe_get(rsi, e_idx)
    atrp_s = safe_get(atrp, s_idx)
    atrp_p = safe_get(atrp, p_idx)
    atrp_e = safe_get(atrp, e_idx)
    volz_s = safe_get(volz, s_idx)
    volz_p = safe_get(volz, p_idx)
    volz_e = safe_get(volz, e_idx)

    agg_stats = {
        "rsi_mean": float(rsi.mean()),
        "rsi_median": float(rsi.median()),
        "rsi_std": float(rsi.std(ddof=0)),
        "atrp_mean": float(atrp.mean()),
        "atrp_median": float(atrp.median()),
        "atrp_std": float(atrp.std(ddof=0)),
        "volz_mean": float(volz.mean()),
        "volz_median": float(volz.median()),
        "volz_std": float(volz.std(ddof=0)),
    }

    ema = calculate_ema(close, span=ema_span)
    vwap_series = calculate_vwap(seg)
    slope_ema = calculate_linreg_slope(ema)
    slope_vwap = calculate_linreg_slope(vwap_series)

    if seg_len > impulse_k:
        impulse = (close - close.shift(impulse_k)).abs() / close.shift(impulse_k)
        impulse = impulse.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        imp_mean = float(impulse.mean())
        imp_max = float(impulse.max())
    else:
        imp_mean = 0.0
        imp_max = 0.0

    base_features = {
        "t_start": pd.Timestamp(ep.t_start),
        "t_end": pd.Timestamp(ep.t_end),
        "direction": ep.direction.value,
        "move_pct": ep.move_pct,
        "duration_bars": ep.duration_bars,
        "max_drawdown_pct": ep.max_drawdown_pct,
        "max_runup_pct": ep.max_runup_pct,
        "rsi_s": rsi_s,
        "rsi_p": rsi_p,
        "rsi_e": rsi_e,
        "atrp_s": atrp_s,
        "atrp_p": atrp_p,
        "atrp_e": atrp_e,
        "volz_s": volz_s,
        "volz_p": volz_p,
        "volz_e": volz_e,
        "slope_ema": slope_ema,
        "slope_vwap": slope_vwap,
        "impulse_mean": imp_mean,
        "impulse_max": imp_max,
    }

    base_features.update(agg_stats)
    return base_features


def build_features_table(
    df: pd.DataFrame,
    episodes: List[Episode],
    ema_span: int = 50,
    impulse_k: int = 8,
) -> pd.DataFrame:
    if not episodes:
        return pd.DataFrame()

    rows = [
        episode_features(df, ep, ema_span=ema_span, impulse_k=impulse_k)
        for ep in episodes
    ]

    features_df = pd.DataFrame(rows)
    if not features_df.empty:
        features_df = features_df.sort_values("t_start").reset_index(drop=True)

    return features_df


# КОНФІГУРАЦІЯ
class EpisodeConfig:
    def __init__(
        self,
        symbol: str = "BTCUSDT",
        timeframe: str = "1m",
        limit: int = 26000,
        move_pct_up: float = 0.01,
        move_pct_down: float = 0.01,
        min_bars: int = 5,
        max_bars: int = 120,
        close_col: str = "close",
        ema_span: int = 50,
        impulse_k: int = 8,
        cluster_k: int = 3,
        pad_bars: int = 5,
        retrace_ratio: float = 0.33,
        require_retrace: bool = True,
        min_gap_bars: int = 0,
        merge_adjacent: bool = True,
        max_episodes: Optional[int] = None,
        adaptive_threshold: bool = False,
        save_results: bool = True,
        create_sample_signals: bool = False,
        # Нові параметри
        volume_z_threshold: float = 2.0,
        rsi_overbought: float = 80.0,
        rsi_oversold: float = 20.0,
        tp_mult: float = 3.0,
        sl_mult: float = 2.0,
        vwap_gate: float = 0.001,
        low_gate: float = 0.001,
        signal_type: str = "random",  # 'random' або 'early_burst'
        early_burst_pad: int = 3,
        weights: Optional[Dict[str, float]] = None,
    ):
        self.symbol = symbol
        self.timeframe = timeframe
        self.limit = limit
        self.move_pct_up = move_pct_up
        self.move_pct_down = move_pct_down
        self.min_bars = min_bars
        self.max_bars = max_bars
        self.close_col = close_col
        self.ema_span = ema_span
        self.impulse_k = impulse_k
        self.cluster_k = cluster_k
        self.pad_bars = pad_bars
        self.retrace_ratio = retrace_ratio
        self.require_retrace = require_retrace
        self.min_gap_bars = min_gap_bars
        self.merge_adjacent = merge_adjacent
        self.max_episodes = max_episodes
        self.adaptive_threshold = adaptive_threshold
        self.save_results = save_results
        self.create_sample_signals = create_sample_signals

        # Нові параметри
        self.volume_z_threshold = volume_z_threshold
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold
        self.tp_mult = tp_mult
        self.sl_mult = sl_mult
        self.vwap_gate = vwap_gate
        self.low_gate = low_gate
        self.signal_type = signal_type
        self.early_burst_pad = early_burst_pad
        self.weights = weights or {"slope_vwap": 0.5, "slope_ema": 0.5}

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> EpisodeConfig:
        return cls(**config_dict)


# ОСНОВНА ФУНКЦІЯ
def main_analysis(
    df: pd.DataFrame,
    config: EpisodeConfig,
    entry_mask: Optional[pd.Series] = None,
    create_sample: bool = False,
) -> Dict[str, Any]:
    # 1) Індикатори (RSI/ATR/atr_pct/volume_z)
    df = ensure_indicators(df)

    # Створюємо тестові сигнали, якщо потрібно
    if create_sample and entry_mask is None:
        if config.signal_type == "early_burst":
            entry_mask = create_early_burst_signals(df, config)
            logger.info(f"Створено {entry_mask.sum()} тестових сигналів (early burst)")
        else:
            entry_mask = create_sample_signals(df, signal_prob=0.05)
            logger.info(f"Створено {entry_mask.sum()} тестових сигналів (випадкові)")

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


# КЛАСТЕРИ ПАТЕРНІВ
def _kmeans_numpy(X: np.ndarray, k: int, iters: int = 50, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    n, d = X.shape

    if n == 0:
        return np.array([], dtype=int)

    centers = X[rng.choice(n, size=min(k, n), replace=False)]

    if centers.shape[0] < k:
        missing = k - centers.shape[0]
        centers = np.vstack(
            [centers, centers[rng.choice(centers.shape[0], missing, replace=True)]]
        )

    labels = np.zeros(n, dtype=int)

    for _ in range(iters):
        dists = np.linalg.norm(X[:, None, :] - centers[None, :, :], axis=2)
        new_labels = dists.argmin(axis=1)

        if np.array_equal(new_labels, labels):
            break

        labels = new_labels

        for c in range(k):
            members = X[labels == c]
            if len(members) > 0:
                centers[c] = members.mean(axis=0)
            else:
                centers[c] = X[rng.integers(0, n)]

    return labels


def cluster_patterns(
    features: pd.DataFrame,
    k: int = 3,
    by_direction: bool = True,
    cols: Optional[List[str]] = None,
    seed: int = 0,
) -> pd.Series:
    if features.empty:
        return pd.Series(dtype=int)

    if cols is None:
        cols = [
            "move_pct",
            "duration_bars",
            "rsi_s",
            "rsi_p",
            "rsi_e",
            "atrp_s",
            "atrp_p",
            "atrp_e",
            "volz_s",
            "volz_p",
            "volz_e",
            "slope_ema",
            "slope_vwap",
            "impulse_mean",
            "impulse_max",
        ]
        cols = [c for c in cols if c in features.columns]

    labels_all = np.full(len(features), -1, dtype=int)

    if by_direction and "direction" in features.columns:
        for direction in [Direction.UP.value, Direction.DOWN.value]:
            mask = (features["direction"] == direction).values
            sub = features.loc[mask, cols]

            if sub.empty:
                continue

            X = sub.values.astype(float)
            mu = X.mean(axis=0, keepdims=True)
            sigma = X.std(axis=0, keepdims=True)
            sigma[sigma == 0.0] = 1.0
            Z = (X - mu) / sigma

            labels = _kmeans_numpy(Z, k=k, iters=50, seed=seed)
            labels_all[mask] = labels
    else:
        X = features[cols].values.astype(float)
        mu = X.mean(axis=0, keepdims=True)
        sigma = X.std(axis=0, keepdims=True)
        sigma[sigma == 0.0] = 1.0
        Z = (X - mu) / sigma
        labels_all = _kmeans_numpy(Z, k=k, iters=50, seed=seed)

    return pd.Series(labels_all, index=features.index, name="cluster")


def reference_ranges(
    features: pd.DataFrame,
    lower_q: float = 0.30,
    upper_q: float = 0.70,
) -> Dict[str, Dict[str, Tuple[float, float]]]:
    out: Dict[str, Dict[str, Tuple[float, float]]] = {}

    if features.empty:
        return out

    metrics = {
        "rsi": ("rsi_s", "rsi_p", "rsi_e", "rsi_mean"),
        "atrp": ("atrp_s", "atrp_p", "atrp_e", "atrp_mean"),
        "volz": ("volz_s", "volz_p", "volz_e", "volz_mean"),
    }

    for direction in [Direction.UP.value, Direction.DOWN.value]:
        group = features[features["direction"] == direction]
        if group.empty:
            continue

        dres: Dict[str, Tuple[float, float]] = {}
        for mname, cols in metrics.items():
            col_vals = pd.concat([group[c] for c in cols if c in group.columns], axis=0)
            if col_vals.empty:
                continue

            low = float(col_vals.quantile(lower_q))
            high = float(col_vals.quantile(upper_q))
            dres[mname] = (low, high)

        out[direction] = dres

    return out


# ПОКРИТТЯ ПОДІЙ СИГНАЛАМИ
def event_hits(
    entry_mask: pd.Series,
    episodes: List[Episode],
    pad: int = 5,
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    if not episodes:
        return pd.DataFrame(columns=["hit", "first_hit_offset", "hit_count"]), {
            "coverage": 0.0,
            "avg_abs_offset": 0.0,
            "misses": 0,
            "total": 0,
        }

    idx = entry_mask.index
    hits_rows = []

    for ep in episodes:
        s = max(0, ep.start_idx - pad)
        e = min(len(idx) - 1, ep.end_idx + pad)
        window = entry_mask.iloc[s : e + 1]
        hit_positions = np.flatnonzero(window.values.astype(bool))

        if hit_positions.size > 0:
            first_rel = int(hit_positions[0])
            first_abs_idx = s + first_rel
            offset = first_abs_idx - ep.start_idx

            hits_rows.append(
                {
                    "t_start": ep.t_start,
                    "t_end": ep.t_end,
                    "direction": ep.direction.value,
                    "hit": True,
                    "first_hit_offset": int(offset),
                    "hit_count": int(hit_positions.size),
                }
            )
        else:
            hits_rows.append(
                {
                    "t_start": ep.t_start,
                    "t_end": ep.t_end,
                    "direction": ep.direction.value,
                    "hit": False,
                    "first_hit_offset": np.nan,
                    "hit_count": 0,
                }
            )

    hits_df = pd.DataFrame(hits_rows)

    coverage = float(hits_df["hit"].mean()) if not hits_df.empty else 0.0

    if hits_df["hit"].any():
        avg_abs_offset = float(
            hits_df.loc[hits_df["hit"], "first_hit_offset"].abs().mean()
        )
    else:
        avg_abs_offset = 0.0

    misses = int((~hits_df["hit"]).sum())
    total = int(len(hits_df))

    stats = {
        "coverage": coverage,
        "avg_abs_offset": avg_abs_offset,
        "misses": misses,
        "total": total,
    }

    return hits_df, stats


def coverage_score_from_hits(
    stats: Dict[str, float],
    w_cover: float = 1.0,
    w_offset: float = 0.2,
    miss_penalty: float = 1.0,
    max_offset_norm: float = 20.0,
) -> float:
    cov = stats.get("coverage", 0.0)
    avg_off = stats.get("avg_abs_offset", 0.0)

    score = (
        w_cover * cov
        - w_offset * (avg_off / max_offset_norm)
        - miss_penalty * (1.0 - cov)
    )

    return float(np.clip(score, -1.0, 1.0))


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

        csv_path = f"bars_{config.symbol}_{config.timeframe}_{config.limit}.csv"
        logger.debug(f"Зчитування даних з CSV: {csv_path}")

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Не знайдено файл: {csv_path}")

        df = read_csv_smart(csv_path)
        logger.debug(f"Розмір DataFrame: {df.shape}, колонки: {list(df.columns)}")

        if df.empty:
            logger.error("Дані не прочитані або порожні!")
            raise SystemExit(1)

        logger.info(f"Аналіз символу: {config.symbol}")

        # Запуск аналізу
        results = main_analysis(
            df,
            config,
            entry_mask=None,
            create_sample=config.create_sample_signals,
        )
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

        # Візуалізація з TP/SL зонами та PnL підписами
        try:
            Path("results").mkdir(exist_ok=True)
            out_file = f"results/{config.symbol}_{tf}_episodes.pdf"

            visualize_episodes(
                df,
                results["episodes"],
                signals_df=(
                    pd.DataFrame(
                        {
                            "ts": results.get("entry_mask")[
                                results.get("entry_mask")
                            ].index,
                            "side": ["BUY"] * int(results.get("entry_mask").sum()),
                            "price": [
                                float(df.loc[t, config.close_col])
                                for t in results.get("entry_mask")[
                                    results.get("entry_mask")
                                ].index
                            ],
                        }
                    )
                    if results.get("entry_mask") is not None
                    and results.get("entry_mask").any()
                    else None
                ),
                save_path=out_file,
                show_signals=True,
                show_anchors=True,
                price_col="close",
                annotate_metrics=True,
                # NEW:
                show_tp_sl_zones=True,
                label_trade_pnl=True,
                # Якщо не передаєш TP/SL у епізодах/ентрі — можеш задати глобально, напр. 2%/1.2%
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

            config_out = f"results/{prefix}_config.json"
            with open(config_out, "w", encoding="utf-8") as f:
                json.dump(config.to_dict(), f, indent=2)

            logger.info(
                f"Результати збережено у теці results:\n  {ep_out}\n  {ft_out}\n  {rr_out}\n  {ht_out}\n  {config_out}"
            )

    end_time = time.time()
    logger.info(f"Аналіз завершено за {end_time - start_time:.2f} секунд")
