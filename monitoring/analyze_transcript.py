"""Аналізатор стенограми (JSONL) для оцінки якості сигналів у лайві.

Функції:
- Читає один або кілька transcript_*.jsonl
- Зв'язує події signal ↔ outcome за id/ref_id
- Рахує метрики по горизонтах: кількість, win_rate, середню/медіанну дохідність
- Групує за символами та (за бажання) за причинами тригерів

Припущення:
- Stage1 не має явного напрямку; політика напряму задається ключем:
  --direction-policy: infer|long|short|none
  * infer: намагаємось вивести напрям за назвами причин (oversold→long, overbought→short)
  * long/short: вважаємо всі сигнали довгими/короткими
  * none: не рахуємо win_rate, лише unsigned метрики

Використання (PowerShell):
  python -m monitoring.analyze_transcript --path .\\data\\transcripts\\transcript_*.jsonl \
      --symbols btcusdt,ethusdt --direction-policy infer --horizons 60,300,900 --notional 100
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from statistics import mean, median
from typing import Any


@dataclass(slots=True)
class Signal:
    id: str
    symbol: str
    ts: int  # ms
    price: float
    reasons: list[str]


@dataclass(slots=True)
class Outcome:
    ref_id: str
    symbol: str
    ts: int  # ms (target time)
    horizon_s: int
    base_price: float
    price: float
    delta: float  # (price/base_price - 1.0)


def _iter_jsonl(paths: Iterable[str]) -> Iterable[dict[str, Any]]:
    for p in paths:
        if not os.path.exists(p):
            continue
    with open(p, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                yield obj


def _infer_direction(reasons: list[str]) -> int | None:
    """Спроба визначити напрямок по списку причин.

    Правила за замовчуванням (налаштовувані через код/патч):
      - рядки, що містять 'oversold', 'bull', 'long' → +1 (лонг)
      - рядки, що містять 'overbought', 'bear', 'short' → -1 (шорт)
      - інакше None
    """
    text = ",".join(reasons).lower()
    if any(k in text for k in ("oversold", "bull", "long")):
        return +1
    if any(k in text for k in ("overbought", "bear", "short")):
        return -1
    return None


def _pct(x: float) -> float:
    return x * 100.0


def analyze(
    *,
    paths: list[str],
    symbols: set[str] | None = None,
    direction_policy: str = "infer",
    horizons: list[int] | None = None,
    notional: float = 100.0,
    min_reasons: int = 0,
) -> dict[str, Any]:
    """Основний аналізатор.

    Args:
        paths: Список шляхів до JSONL-файлів (підтримує glob).
        symbols: Обмежити аналіз цими символами (lowercase).
        direction_policy: 'infer' | 'long' | 'short' | 'none'.
        horizons: Які горизонти агрегувати (секунди). Якщо None, беремо всі наявні.
        notional: Умовний капітал для PnL ($).
        min_reasons: Мінімальна кількість причин, щоб враховувати сигнал.

    Returns:
        Словник з агрегованою статистикою.
    """
    # 1) Збір сигналів та outcomes
    sig_by_id: dict[str, Signal] = {}
    outcomes_by_id: dict[str, list[Outcome]] = defaultdict(list)
    all_horizons: set[int] = set()
    for obj in _iter_jsonl(paths):
        t = obj.get("type")
        if t == "signal":
            sym = str(obj.get("symbol", "")).lower()
            if symbols and sym not in symbols:
                continue
            reasons = list(obj.get("reasons") or [])
            if len(reasons) < min_reasons:
                continue
            sid = str(obj.get("id"))
            sig_by_id[sid] = Signal(
                id=sid,
                symbol=sym,
                ts=int(obj.get("ts", 0)),
                price=float(obj.get("price", 0.0)),
                reasons=reasons,
            )
        elif t == "outcome":
            sym = str(obj.get("symbol", "")).lower()
            if symbols and sym not in symbols:
                continue
            try:
                h = int(obj.get("horizon_s"))
            except Exception:
                continue
            all_horizons.add(h)
            outcomes_by_id[str(obj.get("ref_id"))].append(
                Outcome(
                    ref_id=str(obj.get("ref_id")),
                    symbol=sym,
                    ts=int(obj.get("ts", 0)),
                    horizon_s=h,
                    base_price=float(obj.get("base_price", 0.0)),
                    price=float(obj.get("price", 0.0)),
                    delta=float(obj.get("delta", 0.0)),
                )
            )

    if not sig_by_id:
        return {"error": "no_signals"}

    hlist = sorted(horizons or list(all_horizons))
    # 2) Обчислення метрик
    # Головні агрегати по горизонтах
    overall = {
        h: {"n": 0, "n_dir": 0, "wins": 0, "ret%": [], "abs%": [], "pnl$": []}
        for h in hlist
    }
    # По символах
    by_symbol: dict[str, dict[int, dict[str, Any]]] = defaultdict(
        lambda: {
            h: {"n": 0, "n_dir": 0, "wins": 0, "ret%": [], "abs%": [], "pnl$": []}
            for h in hlist
        }
    )
    # По причинах (кожна причина розглядається незалежно)
    by_reason: dict[str, dict[int, dict[str, Any]]] = defaultdict(
        lambda: {
            h: {"n": 0, "n_dir": 0, "wins": 0, "ret%": [], "abs%": [], "pnl$": []}
            for h in hlist
        }
    )

    def _dir_for(reasons: list[str]) -> int | None:
        if direction_policy == "long":
            return +1
        if direction_policy == "short":
            return -1
        if direction_policy == "infer":
            return _infer_direction(reasons)
        return None

    for sid, sig in sig_by_id.items():
        outs = outcomes_by_id.get(sid) or []
        if not outs:
            continue
        d = _dir_for(sig.reasons)
        for o in outs:
            if o.horizon_s not in overall:
                # не запитаний горизонт
                continue
            # Основні величини
            r_signed = (o.delta * (d if d else 0)) if d else None
            r_abs = abs(o.delta)
            pnl = (
                (notional * (r_signed if r_signed is not None else 0.0)) if d else None
            )

            # Overall
            agg = overall[o.horizon_s]
            agg["n"] += 1
            agg["abs%"].append(_pct(r_abs))
            if d:
                agg["n_dir"] += 1
                agg["ret%"].append(_pct(r_signed))  # type: ignore[arg-type]
                if pnl is not None:
                    agg["pnl$"].append(pnl)
                if r_signed and r_signed > 0:
                    agg["wins"] += 1

            # By symbol
            sagg = by_symbol[sig.symbol][o.horizon_s]
            sagg["n"] += 1
            sagg["abs%"].append(_pct(r_abs))
            if d:
                sagg["n_dir"] += 1
                sagg["ret%"].append(_pct(r_signed))  # type: ignore[arg-type]
                if pnl is not None:
                    sagg["pnl$"].append(pnl)
                if r_signed and r_signed > 0:
                    sagg["wins"] += 1

            # By reason
            for reason in set(sig.reasons):
                ragg = by_reason[reason][o.horizon_s]
                ragg["n"] += 1
                ragg["abs%"].append(_pct(r_abs))
                if d:
                    ragg["n_dir"] += 1
                    ragg["ret%"].append(_pct(r_signed))  # type: ignore[arg-type]
                    if pnl is not None:
                        ragg["pnl$"].append(pnl)
                    if r_signed and r_signed > 0:
                        ragg["wins"] += 1

    def _finalize(bucket: dict[int, dict[str, Any]]) -> dict[int, dict[str, Any]]:
        out: dict[int, dict[str, Any]] = {}
        for h, b in bucket.items():
            n = b["n"]
            n_dir = b["n_dir"]
            wins = b["wins"]
            ret = b["ret%"]
            absret = b["abs%"]
            pnlv = b["pnl$"]
            out[h] = {
                "n": n,
                "n_with_direction": n_dir,
                "win_rate": (wins / n_dir) if n_dir else None,
                "ret_mean": mean(ret) if ret else None,
                "ret_median": median(ret) if ret else None,
                "abs_mean": mean(absret) if absret else None,
                "abs_median": median(absret) if absret else None,
                "pnl_total": round(sum(pnlv), 4) if pnlv else None,
            }
        return out

    result = {
        "overall": _finalize(overall),
        "by_symbol": {sym: _finalize(b) for sym, b in by_symbol.items()},
        "by_reason": {reason: _finalize(b) for reason, b in by_reason.items()},
        "horizons": hlist,
        "signals_total": len(sig_by_id),
        "signals_with_outcomes": sum(1 for v in outcomes_by_id.values() if v),
        "direction_policy": direction_policy,
    }
    return result


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze transcript JSONL")
    p.add_argument("--path", nargs="+", help="Шляхи або glob-патерни до JSONL")
    p.add_argument("--symbols", default="", help="Список символів через кому (lower)")
    p.add_argument(
        "--direction-policy",
        default="infer",
        choices=["infer", "long", "short", "none"],
        help="Політика визначення напряму",
    )
    p.add_argument(
        "--horizons",
        default="",
        help="Список горизонтів у секундах (через кому), якщо пусто — всі наявні",
    )
    p.add_argument("--notional", type=float, default=100.0, help="Нотіонал ($)")
    p.add_argument(
        "--min-reasons", type=int, default=0, help="Фільтр мінімальної кількості причин"
    )
    p.add_argument(
        "--export-csv",
        default="",
        help="Якщо задано — зберегти підсумкові overall у CSV за шляхом",
    )
    return p.parse_args()


def _expand_paths(patterns: list[str]) -> list[str]:
    out: list[str] = []
    for pat in patterns:
        matches = glob.glob(pat)
        out.extend(matches or [pat])
    return out


def _export_overall_csv(data: dict[str, Any], path: str) -> None:
    overall = data.get("overall", {})
    horizons = data.get("horizons", [])
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "horizon_s",
                "n",
                "n_with_direction",
                "win_rate",
                "ret_mean",
                "ret_median",
                "abs_mean",
                "abs_median",
                "pnl_total$",
            ]
        )
        for h in horizons:
            row = overall.get(h, {})
            w.writerow(
                [
                    h,
                    row.get("n"),
                    row.get("n_with_direction"),
                    (
                        round(row.get("win_rate"), 4)
                        if isinstance(row.get("win_rate"), float)
                        else row.get("win_rate")
                    ),
                    (
                        round(row.get("ret_mean"), 4)
                        if isinstance(row.get("ret_mean"), float)
                        else row.get("ret_mean")
                    ),
                    (
                        round(row.get("ret_median"), 4)
                        if isinstance(row.get("ret_median"), float)
                        else row.get("ret_median")
                    ),
                    (
                        round(row.get("abs_mean"), 4)
                        if isinstance(row.get("abs_mean"), float)
                        else row.get("abs_mean")
                    ),
                    (
                        round(row.get("abs_median"), 4)
                        if isinstance(row.get("abs_median"), float)
                        else row.get("abs_median")
                    ),
                    (
                        round(row.get("pnl_total"), 4)
                        if isinstance(row.get("pnl_total"), float)
                        else row.get("pnl_total")
                    ),
                ]
            )


def main() -> None:
    args = _parse_args()
    paths = _expand_paths(args.path)
    symbols = (
        set(s.strip().lower() for s in args.symbols.split(",") if s.strip()) or None
    )
    horizons = (
        [int(x) for x in args.horizons.split(",") if x.strip()]
        if args.horizons
        else None
    )
    data = analyze(
        paths=paths,
        symbols=symbols,
        direction_policy=args.direction_policy,
        horizons=horizons,
        notional=args.notional,
        min_reasons=args.min_reasons,
    )
    # Проста текстова вивідка в консоль
    print(json.dumps(data, ensure_ascii=False, indent=2))
    if args.export_csv:
        _export_overall_csv(data, args.export_csv)


if __name__ == "__main__":
    main()
