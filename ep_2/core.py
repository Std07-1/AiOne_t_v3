# -*- coding: utf-8 -*-
"""Core domain entities and utilities for episode analysis.

This module centralizes shared dataclasses, enums, configuration, and
feature/coverage utilities used across the episodic analysis pipeline.

Separated from episodes.py to reduce duplication and enable cleaner imports
for other modules (find_episodes_v2, main_analysis, visualization, etc.).
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple
import logging
import numpy as np
import pandas as pd

# Indicators expected to be available (light indirection to avoid circular import)
try:  # noqa: SIM105
    from ep_2.indicators import (
        calculate_atr_percent,
        calculate_vwap,
        calculate_ema,
        calculate_linreg_slope,
        segment_data,
        find_impulse_peak,
    )  # type: ignore
except Exception:  # noqa: BLE001

    def calculate_atr_percent(df):  # type: ignore
        return pd.Series(0.0, index=df.index)

    def calculate_vwap(df):  # type: ignore
        return pd.Series(0.0, index=df.index)

    def calculate_ema(series, span=50):  # type: ignore
        return series.ewm(span=span, adjust=False).mean()

    def calculate_linreg_slope(series):  # type: ignore
        return 0.0

    def segment_data(df, start, end):  # type: ignore
        return df.iloc[start : end + 1]

    def find_impulse_peak(df, direction: str):  # type: ignore
        return df.index[-1] if len(df) else pd.Timestamp.utcnow()


logger = logging.getLogger(__name__)


class Direction(str, Enum):
    UP = "up"
    DOWN = "down"


@dataclass
class Episode:
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
        d["direction"] = self.direction.value
        d["t_start"] = self.t_start.isoformat()
        d["t_end"] = self.t_end.isoformat()
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Episode":
        data = data.copy()
        data["direction"] = Direction(data["direction"])
        data["t_start"] = pd.Timestamp(data["t_start"])
        data["t_end"] = pd.Timestamp(data["t_end"])
        return cls(**data)


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
        volume_z_threshold: float = 2.0,
        rsi_overbought: float = 80.0,
        rsi_oversold: float = 20.0,
        tp_mult: float = 3.0,
        sl_mult: float = 2.0,
        vwap_gate: float = 0.001,
        low_gate: float = 0.001,
        **_extra,
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
        self.volume_z_threshold = volume_z_threshold
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold
        self.tp_mult = tp_mult
        self.sl_mult = sl_mult
        self.vwap_gate = vwap_gate
        self.low_gate = low_gate
        if _extra:
            for k, v in _extra.items():
                setattr(self, k, v)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in self.__dict__.items():
            if k.startswith("_"):
                continue
            if callable(v):
                continue
            out[k] = v
        return out


# ---- Coverage & feature utilities ----


def analyze_episode_coverage(
    episodes: List[Episode], entry_mask: pd.Series, df: pd.DataFrame, pad: int = 5
) -> pd.DataFrame:
    if not episodes:
        return pd.DataFrame()
    results = []
    for i, ep in enumerate(episodes):
        segment = df.iloc[ep.start_idx : ep.end_idx + 1]
        peak_time = find_impulse_peak(segment, ep.direction.value)
        peak_idx = df.index.get_loc(peak_time)
        start_search = max(0, peak_idx - pad)
        end_search = min(len(df) - 1, peak_idx + pad)
        window_signals = entry_mask.iloc[start_search : end_search + 1]
        signal_indices = window_signals[window_signals].index.tolist()
        first_signal_offset = None
        if signal_indices:
            first_signal_idx = signal_indices[0]
            first_signal_offset = entry_mask.index.get_loc(first_signal_idx) - peak_idx
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

    def safe_get(series: pd.Series, idx, default=0.0):
        try:
            return float(series.loc[idx])
        except Exception:  # noqa: BLE001
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


def _kmeans_numpy(X: np.ndarray, k: int, iters: int = 50, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    n, _ = X.shape
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
