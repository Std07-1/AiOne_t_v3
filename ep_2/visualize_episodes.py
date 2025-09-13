# ep_2/visualize_episodes_enhanced.py
# -*- coding: utf-8 -*-
"""
Enhanced episode visualization with:
- Clear entry/exit markers (arrows/lines)
- Signal dots by type with distinct markers
- Inline metrics (move %, duration bars) on chart
- Improved styling (colors, sizes, legend, grid)
- Anchor/Confirm/End vertical markers
- More precise time & price scales (nearest snap + interpolation, nicer tick format)
- Optional secondary panel (e.g., RSI)
- NEW: TP/SL zones per trade and PnL labels for each entry→exit pair

Conventions for TP/SL:
- Per-entry preferred fields (absolute or pct): entry["tp"], entry["sl"] (absolute prices)
  or entry["tp_pct"], entry["sl_pct"] (fractions, e.g., 0.02 for +2%).
- Per-episode fallback: ep["tp"], ep["sl"] or ep["tp_pct"], ep["sl_pct"].
- Global fallback (function args): tp_pct, sl_pct.
- Side detection in priority:
    entry["side"] in {"long","short"}  OR
    entry["type"] contains "BUY"/"SELL" OR
    episode["direction"] in {"up","down"}  (up→long, down→short)

Author: Std07-1
"""
from __future__ import annotations

from typing import List, Optional, Dict, Any, Tuple
import os
import math
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from matplotlib.patches import Rectangle
from dataclasses import is_dataclass, asdict

import pandas as pd
import numpy as np

# Logging
import logging

try:
    from rich.console import Console  # type: ignore
    from rich.logging import RichHandler  # type: ignore

    _HAS_RICH = True
except Exception:
    _HAS_RICH = False

logger = logging.getLogger("visualize_episodes")
if not logger.handlers:
    # Установимо детальний рівень логування за замовчуванням
    logger.setLevel(logging.DEBUG)
    if _HAS_RICH:
        _h = RichHandler(console=Console(stderr=True), show_path=False)  # type: ignore[arg-type]
    else:
        _h = logging.StreamHandler()
        _h.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
        )
    try:
        _h.setLevel(logging.DEBUG)
    except Exception:
        pass
    logger.addHandler(_h)
    logger.propagate = False


# --------------------- Helpers ---------------------
def _ep_to_dict(ep: Any) -> Dict[str, Any]:
    if is_dataclass(ep):
        d = asdict(ep)
    elif hasattr(ep, "__dict__"):
        d = dict(ep.__dict__)
    elif isinstance(ep, dict):
        d = dict(ep)
    else:
        d = {}
    dir_val = d.get("direction")
    if hasattr(dir_val, "value"):
        d["direction"] = dir_val.value
    elif isinstance(dir_val, str):
        d["direction"] = dir_val.lower()
    else:
        d["direction"] = None
    return d


def _ensure_points_list(val: Any) -> List[Dict[str, Any]]:
    pts: List[Dict[str, Any]] = []
    if val is None:
        return pts
    for x in val:
        if isinstance(x, dict):
            t = x.get("time") or x.get("t") or x.get("ts")
            p = x.get("price") or x.get("p")
            extra = dict(x)
            extra.pop("time", None)
            extra.pop("t", None)
            extra.pop("ts", None)
            extra.pop("price", None)
            extra.pop("p", None)
        elif isinstance(x, (tuple, list)) and len(x) >= 2:
            t, p = x[0], x[1]
            extra = {}
        else:
            continue
        pts.append(
            {
                "time": pd.Timestamp(t),
                "price": float(p) if p is not None else np.nan,
                **extra,
            }
        )
    return pts


def _snap_index(index: pd.DatetimeIndex, ts: pd.Timestamp) -> int:
    ts_ts = pd.Timestamp(ts)
    pos = index.get_indexer([ts_ts], method="nearest")[0]
    if pos < 0:
        pos = 0
    elif pos >= len(index):
        pos = len(index) - 1
    # Diagnostic hint: report nearest timestamp
    try:
        nearest = index[pos]
        logger.debug(
            "_snap_index: requested=%s, nearest=%s, pos=%d", ts_ts, nearest, pos
        )
    except Exception:
        logger.debug(
            "_snap_index: requested=%s, pos=%d (no nearest available)", ts_ts, pos
        )
    return pos


def _value_at(df: pd.DataFrame, ts: pd.Timestamp, col: str) -> float:
    ts = pd.Timestamp(ts)
    if ts in df.index:
        return float(df.loc[ts, col])
    idx = df.index
    i = idx.searchsorted(ts)
    if i == 0:
        return float(df.iloc[0][col])
    if i >= len(idx):
        return float(df.iloc[-1][col])
    t0, t1 = idx[i - 1], idx[i]
    y0, y1 = float(df.loc[t0, col]), float(df.loc[t1, col])
    frac = (ts - t0) / (t1 - t0)
    try:
        frac = float(frac)
    except Exception:
        frac = 0.0
    logger.debug(
        "_value_at: ts=%s, left=%s, right=%s, frac=%s, val0=%s, val1=%s",
        ts,
        t0,
        t1,
        frac,
        y0,
        y1,
    )
    return y0 + (y1 - y0) * frac


def _detect_side(entry: Dict[str, Any], ep_dir: Optional[str]) -> str:
    side = str(entry.get("side", "")).lower()
    etype = str(entry.get("type", "")).upper()
    if side in {"long", "short"}:
        return side
    if "BUY" in etype:
        return "long"
    if "SELL" in etype:
        return "short"
    if ep_dir in {"up", "down"}:
        return "long" if ep_dir == "up" else "short"
    return "long"  # default


def _tp_sl_prices(
    entry: Dict[str, Any],
    ep: Dict[str, Any],
    entry_price: float,
    default_tp_pct: Optional[float],
    default_sl_pct: Optional[float],
    side: str,
) -> Tuple[Optional[float], Optional[float]]:
    # Absolute first (entry-level)
    tp = entry.get("tp")
    sl = entry.get("sl")
    # Percent at entry-level
    tp_pct = entry.get("tp_pct")
    sl_pct = entry.get("sl_pct")

    # Episode-level fallback
    if tp is None:
        tp = ep.get("tp")
    if sl is None:
        sl = ep.get("sl")
    if tp_pct is None:
        tp_pct = ep.get("tp_pct")
    if sl_pct is None:
        sl_pct = ep.get("sl_pct")

    # Function-level fallback (pct)
    if tp is None and tp_pct is None and default_tp_pct is not None:
        tp_pct = default_tp_pct
    if sl is None and sl_pct is None and default_sl_pct is not None:
        sl_pct = default_sl_pct

    # Resolve pct → absolute
    def pct_to_abs(pct: float, price: float, side: str, is_tp: bool) -> float:
        if side == "long":
            return price * (1 + pct) if is_tp else price * (1 - pct)
        else:
            # for short, TP is below entry, SL above
            return price * (1 - pct) if is_tp else price * (1 + pct)

    if tp is None and isinstance(tp_pct, (int, float)):
        tp = pct_to_abs(float(tp_pct), entry_price, side, is_tp=True)
    if sl is None and isinstance(sl_pct, (int, float)):
        sl = pct_to_abs(float(sl_pct), entry_price, side, is_tp=False)
    logger.debug(
        "_tp_sl_prices: entry_price=%s, side=%s, resolved_tp=%s, resolved_sl=%s, tp_pct=%s, sl_pct=%s",
        entry_price,
        side,
        tp,
        sl,
        tp_pct,
        sl_pct,
    )

    return (
        float(tp) if tp is not None else None,
        float(sl) if sl is not None else None,
    )


def _pnl_pct(entry_p: float, exit_p: float, side: str) -> float:
    if side == "long":
        return (exit_p - entry_p) / entry_p
    else:
        return (entry_p - exit_p) / entry_p


def attach_signals_to_episodes(
    episodes: List[Dict[str, Any]],
    signals_df: pd.DataFrame,
    df: pd.DataFrame,
    anchor_tolerance_bars: int = 30,
) -> List[Dict[str, Any]]:
    """
    Для кожного епізоду додає список сигналів (records) та обчислює лаг у барах відносно t_anchor (або t_start).

    Очікується signals_df з колонками ['ts','side','price'] або з індексом timestamp.
    """
    if signals_df is None or signals_df.empty:
        return episodes

    # Нормалізуємо signals_df: переконаємось, що є колонка 'ts' з pd.Timestamp
    sigs = signals_df.copy()
    if "ts" not in sigs.columns:
        # спробуємо взяти індекс
        sigs = sigs.reset_index()
        sigs = sigs.rename(columns={sigs.columns[0]: "ts"})
    sigs["ts"] = pd.to_datetime(sigs["ts"])

    idx_to_pos = {ts: i for i, ts in enumerate(df.index)}

    out_eps = []
    for ep in episodes:
        ep = dict(ep)
        t_start = ep.get("t_start") or (
            df.index[int(ep.get("start_idx"))]
            if ep.get("start_idx") is not None
            else None
        )
        t_end = ep.get("t_end") or (
            df.index[int(ep.get("end_idx"))] if ep.get("end_idx") is not None else None
        )
        t_anchor = ep.get("t_anchor") or t_start

        # сигнали у вікні епізоду (додав невелику дельту 5 хв)
        window_lo = (
            pd.Timestamp(t_start) - pd.Timedelta(minutes=5)
            if t_start is not None
            else None
        )
        window_hi = (
            pd.Timestamp(t_end) + pd.Timedelta(minutes=5) if t_end is not None else None
        )
        if window_lo is not None and window_hi is not None:
            mask = (sigs["ts"] >= window_lo) & (sigs["ts"] <= window_hi)
            found = sigs.loc[mask].copy()
        else:
            found = sigs.iloc[0:0].copy()

        # якщо пусто — шукати близько до t_anchor у межах anchor_tolerance_bars
        if found.empty and t_anchor is not None:
            a_pos = idx_to_pos.get(pd.Timestamp(t_anchor))
            if a_pos is not None:
                lo = max(0, a_pos - anchor_tolerance_bars)
                hi = min(len(df.index) - 1, a_pos + anchor_tolerance_bars)
                t_lo, t_hi = df.index[lo], df.index[hi]
                mask = (sigs["ts"] >= t_lo) & (sigs["ts"] <= t_hi)
                found = sigs.loc[mask].copy()

        # обчислення лагу в барах відносно t_anchor (або t_start)
        ref_ts = (
            pd.Timestamp(t_anchor)
            if t_anchor is not None
            else (pd.Timestamp(t_start) if t_start is not None else None)
        )
        ref_pos = idx_to_pos.get(ref_ts) if ref_ts is not None else None
        if not found.empty and ref_pos is not None:
            found["lag_bars"] = found["ts"].map(
                lambda t: idx_to_pos.get(pd.Timestamp(t), ref_pos) - ref_pos
            )
        else:
            found["lag_bars"] = np.nan

        # приводимо колонки до очікуваного формату записів
        records = []
        for _, r in found.iterrows():
            records.append(
                {
                    "ts": pd.Timestamp(r["ts"]),
                    "time": pd.Timestamp(r["ts"]),
                    "price": float(
                        r.get("price")
                        if "price" in r.index
                        else _value_at(df, r["ts"], "close")
                    ),
                    "side": str(r.get("side", "")).upper(),
                    "tag": r.get("tag") if "tag" in r.index else None,
                    "lag_bars": (
                        int(r["lag_bars"]) if (not pd.isna(r["lag_bars"])) else np.nan
                    ),
                }
            )

        ep["signals"] = records
        out_eps.append(ep)

    return out_eps


# --------------------- Main ---------------------
def visualize_episodes(
    df: pd.DataFrame,
    episodes: List[Any],
    save_path: Optional[str] = None,
    *,
    signals_df: Optional[pd.DataFrame] = None,
    show_signals: bool = True,
    show_anchors: bool = True,
    price_col: str = "close",
    show_trade_lines: bool = True,
    show_episode_arrow: bool = True,
    annotate_metrics: bool = True,
    rsi_panel: Optional[pd.Series] = None,  # optional second panel (0-100)
    rsi_name: str = "RSI",
    # NEW: TP/SL & PnL options
    show_tp_sl_zones: bool = True,
    label_trade_pnl: bool = True,
    tp_pct: Optional[float] = None,
    sl_pct: Optional[float] = None,
    tp_color: str = "#00BCD4",  # cyan-ish
    sl_color: str = "#FFB74D",  # orange-ish
    tp_alpha: float = 0.12,
    sl_alpha: float = 0.12,
) -> None:
    """
    Draw episodes on a price chart with crisp markers/lines and inline metrics.

    Args:
        df: DataFrame with DatetimeIndex and price in `price_col`.
        episodes: List of Episode-like objects/dicts.
        save_path: If provided, saves (PNG/PDF based on extension).
        show_signals: Render per-type signal dots.
        show_anchors: Render t_anchor/t_confirm/t_end vertical lines.
        price_col: Name of price column.
        show_trade_lines: Connect entry->exit (or to episode end) with thin line.
        show_episode_arrow: Arrow from t_start to t_end following close values.
        annotate_metrics: Inline badge near peak with move% and duration bars.
        rsi_panel: Optional RSI series to render as a separate panel.
        rsi_name: Label for RSI panel.
        show_tp_sl_zones: Draw TP/SL rectangles from entry time to exit/end.
        label_trade_pnl: Put PnL % at midpoint of each trade line.
        tp_pct/sl_pct: Default % (0.02 = 2%) if not supplied per entry/episode.
        tp_color/sl_color: Colors for TP/SL zones. tp_alpha/sl_alpha: transparency.
    """
    logger.debug("Початок візуалізації епізодів")
    try:
        logger.debug(
            "DF: rows=%d, cols=%d, index_type=%s",
            len(df),
            len(df.columns),
            type(df.index),
        )
        if not isinstance(df.index, pd.DatetimeIndex):
            logger.debug(
                "Увага: індекс df не є DatetimeIndex (тип: %s)", type(df.index)
            )
            raise ValueError("df must have a DatetimeIndex")
    except Exception as e:
        logger.debug("Помилка при перевірці df: %s", e, exc_info=True)
        raise

    # ---------- Figure layout ----------
    if rsi_panel is None:
        fig, ax = plt.subplots(figsize=(16, 9))
        axs = [ax]
    else:
        fig = plt.figure(figsize=(16, 10))
        gs = fig.add_gridspec(2, 1, height_ratios=[3, 1], hspace=0.05)
        ax = fig.add_subplot(gs[0, 0])
        ax_rsi = fig.add_subplot(gs[1, 0], sharex=ax)
        axs = [ax, ax_rsi]

    # ---------- Styling ----------
    price_lw = 0.9
    arrow_lw = 1.0
    span_alpha = 0.09
    entry_ms = 54
    exit_ms = 54
    signal_ms = 34

    dir_colors = {"up": "#1DB954", "down": "#E53935", None: "#888888"}

    # price line
    ax.plot(df.index, df[price_col].astype(float), label="Close", linewidth=price_lw)

    # Нормалізуємо episodes у список словників і при потребі приєднаємо сигнали
    eps: List[Dict[str, Any]] = [_ep_to_dict(ep) for ep in (episodes or [])]
    if signals_df is not None and not signals_df.empty:
        try:
            eps = attach_signals_to_episodes(eps, signals_df, df)
            logger.debug("Прикріплено сигнали до епізодів: %d епізодів" % len(eps))
        except Exception as e:
            logger.debug(
                "Не вдалось прикріпити сигнали до епізодів: %s", e, exc_info=True
            )

    # Improved grid
    for a in axs:
        a.grid(True, which="both", axis="both", alpha=0.25, linewidth=0.6)

    # Nicer x ticks
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=6, maxticks=12))
    plt.setp(ax.get_xticklabels(), rotation=15, ha="right")

    # Smarter y formatting (avoid scientific)
    def _auto_price_formatter(x, _pos):
        mag = abs(x)
        if mag >= 100:
            return f"{x:,.0f}"
        elif mag >= 10:
            return f"{x:,.2f}"
        elif mag >= 1:
            return f"{x:,.3f}"
        else:
            return f"{x:,.6f}"

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_auto_price_formatter))
    ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=8, prune=None))

    legend_handles: Dict[str, Any] = {}

    def _legend_once(h, label: str):
        if label not in legend_handles and h is not None:
            legend_handles[label] = h

    # ---------- Draw episodes ----------
    for i, ep in enumerate(eps or []):
        t_start = ep.get("t_start")
        t_end = ep.get("t_end")
        logger.debug("Епізод #%d: t_start=%s, t_end=%s", i, t_start, t_end)
        if t_start is None or t_end is None:
            logger.debug("Епізод #%d: пропуск - відсутній t_start або t_end", i)
            continue
        t_start = pd.Timestamp(t_start)
        t_end = pd.Timestamp(t_end)
        direction = ep.get("direction")
        color = dir_colors.get(direction, "#888888")

        # Background span
        span = ax.axvspan(t_start, t_end, color=color, alpha=span_alpha, lw=0)
        _legend_once(span, "Episode zone")

        # Episode arrow
        if show_episode_arrow:
            try:
                y0 = _value_at(df, t_start, price_col)
                y1 = _value_at(df, t_end, price_col)
                ax.annotate(
                    "",
                    xy=(mdates.date2num(t_end), y1),
                    xytext=(mdates.date2num(t_start), y0),
                    arrowprops=dict(
                        arrowstyle="-|>",
                        lw=arrow_lw,
                        color=color,
                        shrinkA=4,
                        shrinkB=4,
                        alpha=0.95,
                    ),
                )
            except Exception as e:
                logger.debug("Arrow failed for episode %s: %s", i, e, exc_info=True)

        # Peak label with metrics
        if annotate_metrics:
            peak_idx = ep.get("peak_idx")
            move_pct = ep.get("move_pct")
            label = None
            if isinstance(move_pct, (int, float)) and math.isfinite(move_pct):
                label = f"{('▲' if direction=='up' else '▼') if direction in ('up','down') else ''} {move_pct:.2%}"
            try:
                start_pos = _snap_index(df.index, t_start)
                end_pos = _snap_index(df.index, t_end)
                duration = max(1, end_pos - start_pos + 1)
            except Exception:
                duration = None
            if duration:
                label = f"{label or ''} • {duration} bars".strip(" • ")
            try:
                if isinstance(peak_idx, int) and 0 <= peak_idx < len(df):
                    peak_time = df.index[peak_idx]
                    peak_price = float(df.iloc[peak_idx][price_col])
                else:
                    midnum = (mdates.date2num(t_start) + mdates.date2num(t_end)) / 2.0
                    peak_time = mdates.num2date(midnum)
                    peak_price = (
                        _value_at(df, t_start, price_col)
                        + _value_at(df, t_end, price_col)
                    ) / 2.0
                dy = 18 if direction == "up" else -22
                if label:
                    ax.annotate(
                        label,
                        xy=(peak_time, peak_price),
                        xytext=(0, dy),
                        textcoords="offset points",
                        ha="center",
                        fontsize=8,
                        bbox=dict(
                            boxstyle="round,pad=0.25",
                            fc="white",
                            ec=color,
                            lw=0.8,
                            alpha=0.95,
                        ),
                        arrowprops=dict(arrowstyle="->", color=color, lw=0.7),
                    )
            except Exception as e:
                logger.debug(
                    "Metric annotation failed on episode %s: %s", i, e, exc_info=True
                )

        # Entries / Exits
        entries = _ensure_points_list(
            ep.get("entries") or ep.get("entry_points") or ep.get("signals_in")
        )
        exits = _ensure_points_list(
            ep.get("exits") or ep.get("exit_points") or ep.get("signals_out")
        )
        sigs = ep.get("signals") or []
        logger.debug(
            "Епізод #%d: entries=%d, exits=%d, signals=%d",
            i,
            len(entries),
            len(exits),
            len(sigs) if isinstance(sigs, list) else 0,
        )
        # Діагностика формату епізоду: короткі приклади та ключі
        try:
            logger.debug(
                "Епізод #%d keys: %s",
                i,
                list(ep.keys()) if isinstance(ep, dict) else repr(type(ep)),
            )
            # Показати до 3 прикладів точок, щоб виявити проблеми з time/price/NaN
            logger.debug(
                "Епізод #%d entries sample: %s",
                i,
                [
                    {
                        "time": (
                            str(p.get("time"))
                            if isinstance(p, dict)
                            else str(p[0]) if p else None
                        ),
                        "price": (
                            p.get("price")
                            if isinstance(p, dict)
                            else (p[1] if len(p) > 1 else None)
                        ),
                        **{
                            k: v
                            for k, v in (p.items() if isinstance(p, dict) else {})
                            if k not in ("time", "price")
                        },
                    }
                    for p in entries[:3]
                ],
            )
            logger.debug(
                "Епізод #%d exits sample: %s",
                i,
                [
                    {
                        "time": (
                            str(p.get("time"))
                            if isinstance(p, dict)
                            else str(p[0]) if p else None
                        ),
                        "price": (
                            p.get("price")
                            if isinstance(p, dict)
                            else (p[1] if len(p) > 1 else None)
                        ),
                    }
                    for p in exits[:3]
                ],
            )
            if (
                not entries
                and not exits
                and (not sigs or (isinstance(sigs, list) and len(sigs) == 0))
            ):
                # Трохи сирого епізоду — обмежимо до перших 12 ключів, щоб не засмічувати лог
                if isinstance(ep, dict):
                    sample_keys = list(ep.keys())[:12]
                    sample = {k: ep.get(k) for k in sample_keys}
                else:
                    sample = repr(ep)
                logger.debug("Епізод #%d: raw ep sample (truncated): %s", i, sample)
        except Exception as e:
            logger.debug(
                "Епізод #%d: diagnostics logging failed: %s", i, e, exc_info=True
            )
        # Якщо немає явних точок входу/виходу — спробуємо створити синтетичні
        if not entries and not exits:
            try:
                si = ep.get("start_idx")
                ei = ep.get("end_idx")
                created = False
                if si is not None and ei is not None:
                    # перевіримо індекси у межах df
                    if isinstance(si, (int, float)):
                        si_i = int(si)
                    else:
                        si_i = None
                    if isinstance(ei, (int, float)):
                        ei_i = int(ei)
                    else:
                        ei_i = None
                    if (
                        si_i is not None
                        and ei_i is not None
                        and 0 <= si_i < len(df)
                        and 0 <= ei_i < len(df)
                    ):
                        en = {
                            "time": df.index[si_i],
                            "price": float(df.iloc[si_i][price_col]),
                        }
                        ex = {
                            "time": df.index[ei_i],
                            "price": float(df.iloc[ei_i][price_col]),
                        }
                        entries = [en]
                        exits = [ex]
                        created = True
                        logger.debug(
                            "Епізод #%d: створено синтетичні точки з start_idx/end_idx: %s -> %s",
                            i,
                            en,
                            ex,
                        )
                if (
                    not created
                    and ep.get("t_start") is not None
                    and ep.get("t_end") is not None
                ):
                    try:
                        en = {
                            "time": pd.Timestamp(ep.get("t_start")),
                            "price": _value_at(df, ep.get("t_start"), price_col),
                        }
                        ex = {
                            "time": pd.Timestamp(ep.get("t_end")),
                            "price": _value_at(df, ep.get("t_end"), price_col),
                        }
                        entries = [en]
                        exits = [ex]
                        logger.debug(
                            "Епізод #%d: створено синтетичні точки з t_start/t_end: %s -> %s",
                            i,
                            en,
                            ex,
                        )
                    except Exception as e:
                        logger.debug(
                            "Епізод #%d: не вдалось створити точки з t_start/t_end: %s",
                            i,
                            e,
                            exc_info=True,
                        )
            except Exception as e:
                logger.debug(
                    "Епізод #%d: помилка при створенні синтетичних точок: %s",
                    i,
                    e,
                    exc_info=True,
                )
        if entries:
            h1 = ax.scatter(
                [p["time"] for p in entries],
                [p["price"] for p in entries],
                s=entry_ms,
                marker="^",
                edgecolor="#114422",
                facecolor="#1DB954",
                linewidths=0.6,
                alpha=0.98,
                zorder=6,
            )
            _legend_once(h1, "Entry")
        if exits:
            h2 = ax.scatter(
                [p["time"] for p in exits],
                [p["price"] for p in exits],
                s=exit_ms,
                marker="v",
                edgecolor="#661a1a",
                facecolor="#E53935",
                linewidths=0.6,
                alpha=0.98,
                zorder=6,
            )
            _legend_once(h2, "Exit")

        # Pair up trades in chronological order (min(len(entries), len(exits)))
        pairs = []
        if entries or exits:
            e_sorted = sorted(entries, key=lambda x: x["time"])
            x_sorted = sorted(exits, key=lambda x: x["time"])
            n = min(len(e_sorted), len(x_sorted)) if x_sorted else len(e_sorted)
            for k in range(n):
                en = e_sorted[k]
                ex = (
                    x_sorted[k]
                    if x_sorted
                    else {"time": t_end, "price": _value_at(df, t_end, price_col)}
                )
                pairs.append((en, ex))

        # TP/SL zones + trade lines + PnL labels
        for en, ex in pairs:
            en_t, en_p = pd.Timestamp(en["time"]), float(en["price"])
            ex_t, ex_p = pd.Timestamp(ex["time"]), float(ex["price"])
            side = _detect_side(en, direction)

            # Trade connector
            if show_trade_lines:
                ax.plot(
                    [en_t, ex_t],
                    [en_p, ex_p],
                    linewidth=1.1,
                    linestyle="-",
                    color="#455A64",
                    alpha=0.95,
                    zorder=5,
                )

            # TP/SL zone rectangles
            if show_tp_sl_zones:
                tp_abs, sl_abs = _tp_sl_prices(en, ep, en_p, tp_pct, sl_pct, side)
                x0 = mdates.date2num(en_t)
                x1 = mdates.date2num(ex_t if ex_t is not None else t_end)
                width = max(1e-9, x1 - x0)

                def _add_rect(y0, y1, color, alpha):
                    y_low, y_high = (y0, y1) if y1 >= y0 else (y1, y0)
                    rect = Rectangle(
                        (x0, y_low),
                        width,
                        y_high - y_low,
                        facecolor=color,
                        edgecolor=color,
                        alpha=alpha,
                        linewidth=0.0,
                        zorder=2,
                    )
                    ax.add_patch(rect)

                if tp_abs is not None:
                    _add_rect(en_p, tp_abs, tp_color, tp_alpha)
                    _legend_once(
                        ax.add_patch(
                            Rectangle((0, 0), 0, 0, facecolor=tp_color, alpha=tp_alpha)
                        ),
                        "TP zone",
                    )

                if sl_abs is not None:
                    _add_rect(en_p, sl_abs, sl_color, sl_alpha)
                    _legend_once(
                        ax.add_patch(
                            Rectangle((0, 0), 0, 0, facecolor=sl_color, alpha=sl_alpha)
                        ),
                        "SL zone",
                    )

            # PnL label at midpoint
            if label_trade_pnl:
                mid_t_num = (mdates.date2num(en_t) + mdates.date2num(ex_t)) / 2.0
                mid_t = mdates.num2date(mid_t_num)
                mid_p = (en_p + ex_p) / 2.0
                pnl = _pnl_pct(en_p, ex_p, side)
                pnl_txt = f"{'+' if pnl >= 0 else ''}{pnl*100:.2f}%"
                ax.annotate(
                    pnl_txt,
                    xy=(mid_t, mid_p),
                    xytext=(0, -12 if pnl >= 0 else 12),
                    textcoords="offset points",
                    ha="center",
                    fontsize=8,
                    bbox=dict(
                        boxstyle="round,pad=0.25",
                        fc="#E8F5E9" if pnl >= 0 else "#FFEBEE",
                        ec="#2E7D32" if pnl >= 0 else "#C62828",
                        lw=0.8,
                        alpha=0.98,
                    ),
                    zorder=7,
                )

        # Signals by type
        if show_signals:
            sigs = ep.get("signals") or []
            if isinstance(sigs, list) and sigs:
                by_type: Dict[str, Dict[str, list]] = {}
                for s in sigs:
                    if not isinstance(s, dict):
                        continue
                    st = str(s.get("type", "SIG")).upper()
                    by_type.setdefault(st, {"t": [], "p": []})
                    by_type[st]["t"].append(pd.Timestamp(s.get("time")))
                    by_type[st]["p"].append(
                        float(s.get("price")) if s.get("price") is not None else np.nan
                    )
                for stype, data in by_type.items():
                    if "BUY" in stype:
                        ec, fc, marker = "#0b4f0f", "#66BB6A", "o"
                    elif "SELL" in stype:
                        ec, fc, marker = "#7a1111", "#EF5350", "o"
                    elif "ALERT" in stype or "SIGNAL" in stype:
                        ec, fc, marker = "#333366", "#7986CB", "D"
                    else:
                        ec, fc, marker = "#424242", "#BDBDBD", "o"
                    h = ax.scatter(
                        data["t"],
                        data["p"],
                        s=signal_ms,
                        marker=marker,
                        edgecolor=ec,
                        facecolor=fc,
                        linewidths=0.5,
                        alpha=0.9,
                        zorder=4,
                    )
                    _legend_once(h, stype.capitalize())

        # Anchor/Confirm/End
        if show_anchors:
            for key, color_key, lbl in [
                ("t_anchor", "#0066CC", "Anchor"),
                ("t_confirm", "#8E24AA", "Confirm"),
                ("t_end", "#212121", "Local End"),
            ]:
                ts = ep.get(key)
                if ts is not None:
                    line = ax.axvline(
                        pd.Timestamp(ts),
                        color=color_key,
                        linewidth=0.9,
                        alpha=0.85,
                        linestyle="-",
                    )
                    _legend_once(line, lbl)

    # ---------- RSI panel (optional) ----------
    if rsi_panel is not None:
        ax_rsi.set_ylabel(rsi_name)
        ax_rsi.plot(rsi_panel.index, rsi_panel.values, linewidth=0.9)
        ax_rsi.set_ylim(0, 100)
        ax_rsi.yaxis.set_major_locator(mticker.MultipleLocator(20))
        ax_rsi.grid(True, alpha=0.25, linewidth=0.6)
        ax_rsi.axhline(30, color="#9E9E9E", linewidth=0.7, linestyle="--")
        ax_rsi.axhline(70, color="#9E9E9E", linewidth=0.7, linestyle="--")

    # ---------- Titles & legend ----------
    ax.set_title("Візуалізація епізодів", fontsize=13)
    ax.set_xlabel("Час")
    ax.set_ylabel("Ціна")

    # Переклад міток легенди за простим мапінгом, якщо присутні
    if legend_handles:
        labels = []
        for k in legend_handles.keys():
            lab = k
            if k == "Entry":
                lab = "Вхід"
            elif k == "Exit":
                lab = "Вихід"
            elif k == "TP zone":
                lab = "Зона TP"
            elif k == "SL zone":
                lab = "Зона SL"
            elif k == "Episode zone":
                lab = "Епізод"
            labels.append(lab)
        ax.legend(
            legend_handles.values(),
            labels,
            loc="upper left",
            frameon=True,
            fontsize=8,
            ncol=2,
        )

    plt.tight_layout()

    # ---------- Save/Show ----------
    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        ext = os.path.splitext(save_path)[1].lower()
        dpi = 220 if ext in (".png", ".jpg", ".jpeg") else None
        fig.savefig(save_path, dpi=dpi, bbox_inches="tight")
        logger.info("Saved figure to %s", save_path)
    plt.show()
