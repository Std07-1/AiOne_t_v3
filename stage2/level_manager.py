"""LevelSystem v2 — менеджер рівнів (агрегація + нормалізація).

Можливості:
    • Збір рівнів з різних джерел: daily / intraday / pivots / volume profile / Anchored VWAP
    • Вага + band_pct + злиття близьких рівнів за відносним ε
    • Пошук найближчих support / resistance (score-aware)
    • Seed денними рівнями при відсутності історії
    • Corridor / evidence API для Stage2

Призначення: централізоване управління та збагачення рівнів для QDE / Stage2.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

from stage1.indicators.levels import calculate_global_levels

# Логування
logger = logging.getLogger("app.stage2.level_manager")
if not logger.handlers:  # захист від повторної ініціалізації
    logger.setLevel(logging.INFO)
    logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
    logger.propagate = False


@dataclass
class Level:
    """Єдиний формат зберігання рівня.

    Attributes:
        value: Значення ціни рівня.
        score: Вага/якість рівня в [0..∞); згодом нормалізується у [0..1].
        source: Джерело ("daily"|"intraday"|"volume"|"vwap"|"pivot"|"merged"|...).
        tf: Таймфрейм джерела ("1m"|"5m"|"1h"|"1d"|...).
        touches: Кількість торкань рівня ціною (можна інкрементувати в runtime).
        last_touch_ts: Час останнього торкання (epoch seconds), якщо ведете.
        band_pct: «Ширина» робочої зони навколо рівня у %, відносно поточної ціни.
    """

    value: float
    score: float = 1.0
    source: str = "unknown"
    tf: str = "NA"
    touches: int = 0
    last_touch_ts: float | None = None
    band_pct: float = 0.0


class LevelManager:
    """Менеджер рівнів (LevelSystem v2).

    Підтримує сумісність зі старим API (daily/intraday списки) і нову «книгу» рівнів.
    """

    def __init__(self) -> None:
        self.daily_levels: dict[str, list[float]] = {}
        self.intraday_levels: dict[str, list[float]] = {}
        # Нова книга рівнів
        self.level_book: dict[str, list[Level]] = {}
        # Кеш мета-параметрів символу (напр., ATR%, tick_size)
        self.symbol_meta: dict[str, dict[str, float]] = {}
        self.logger = logger

    # Старі методи: бек-сумісність і синхронізація в книгу рівнів

    def set_daily_levels(self, symbol: str, levels: list[float]) -> None:
        """Зберігає глобальні денні рівні для символу й синхронізує в книгу рівнів.

        Args:
            symbol: Символ (напр., "btcusdt").
            levels: Масив рівнів (float).
        """
        sym = symbol.lower()
        self.daily_levels[sym] = list(map(float, levels))
        self.logger.debug("Daily levels set for %s: %s", symbol, levels[:3])
        book = self.level_book.setdefault(sym, [])
        for v in levels:
            book.append(
                Level(value=float(v), score=1.0, source="daily", tf="1d", band_pct=0.10)
            )

    def set_intraday_levels(
        self, symbol: str, df: pd.DataFrame, window: int = 20
    ) -> None:
        """Розраховує та зберігає внутрішньоденні рівні; додає їх до книги рівнів.

        Args:
            symbol: Символ (напр., "btcusdt").
            df: DataFrame зі стовпцями ціни (очікується, що сумісний із `calculate_global_levels`).
            window: Вікно для внутрішньоденних рівнів.
        """
        sym = symbol.lower()
        levels = calculate_global_levels(df, window)
        self.intraday_levels[sym] = levels
        self.logger.debug("Intraday levels set for %s: %s", symbol, levels[:3])
        book = self.level_book.setdefault(sym, [])
        for v in levels:
            book.append(
                Level(
                    value=float(v), score=1.2, source="intraday", tf="1m", band_pct=0.08
                )
            )

    def get_all_levels(self, symbol: str) -> list[float]:
        """Повертає всі доступні рівні (legacy + книга рівнів) для символу.

        Args:
            symbol: Символ.

        Returns:
            Відсортований масив рівнів (унікальні значення).
        """
        sym = symbol.lower()
        all_levels: list[float] = []
        if sym in self.daily_levels:
            all_levels.extend(self.daily_levels[sym])
        if sym in self.intraday_levels:
            all_levels.extend(self.intraday_levels[sym])
        if sym in self.level_book:
            all_levels.extend([round(lv.value, 12) for lv in self.level_book[sym]])
        # унікалізація + сортування
        return sorted(set(all_levels))

    def get_nearest_levels(
        self, symbol: str, price: float
    ) -> tuple[float | None, float | None]:
        """Повертає найближчі рівні підтримки/опору.

        Використовує нову книгу рівнів, якщо вона є; інакше — legacy-списки.

        Args:
            symbol: Символ.
            price: Поточна ціна.

        Returns:
            (support, resistance) — найближчі значення цін або (None, None).
        """
        sym = symbol.lower()
        # Новий двигун
        if sym in self.level_book and self.level_book[sym]:
            return self._nearest_by_score(sym, price)

        # Legacy шлях (бек-сумісність)
        all_levels: list[float] = []
        if sym in self.daily_levels:
            all_levels.extend(self.daily_levels[sym])
        if sym in self.intraday_levels:
            all_levels.extend(self.intraday_levels[sym])
        if not all_levels:
            self.logger.info("[levels] empty for %s", symbol)
            return None, None

        all_levels = sorted(set(all_levels))
        supports = [lvl for lvl in all_levels if lvl < price]
        resistances = [lvl for lvl in all_levels if lvl > price]
        support = max(supports) if supports else None
        resistance = min(resistances) if resistances else None
        return support, resistance

    def ensure_and_get_nearest_levels(
        self,
        symbol: str,
        price: float,
        daily_low: float | None,
        daily_high: float | None,
    ) -> tuple[float | None, float | None]:
        """Повертає nearest; якщо пусто — підсідає денними рівнями, тоді повторює запит.

        Args:
            symbol: Символ.
            price: Поточна ціна.
            daily_low: Денний low (як seed, якщо пусто).
            daily_high: Денний high (як seed, якщо пусто).

        Returns:
            (support, resistance).
        """
        s, r = self.get_nearest_levels(symbol, price)
        if s is None and r is None:
            seed = [v for v in [daily_low, daily_high] if v is not None]
            if seed:
                self.set_daily_levels(symbol, seed)
                self.logger.info(
                    "[levels] Seeded %s with daily levels: %s", symbol, seed
                )
                return self.get_nearest_levels(symbol, price)
        return s, r

    # V2: мета, інкрементальне оновлення та допоміжні алгоритми

    def update_meta(
        self,
        symbol: str,
        atr_pct: float | None = None,
        tick_size: float | None = None,
    ) -> None:
        """Оновлює мета-параметри символу (для контролю ε і band).

        Args:
            symbol: Символ.
            atr_pct: ATR у %, наприклад 0.6 (=0.6%).
            tick_size: Мінімальний крок ціни (може бути використаний як нижня межа ε).
        """
        sym = symbol.lower()
        meta = self.symbol_meta.setdefault(sym, {})
        if atr_pct is not None:
            meta["atr_pct"] = float(atr_pct)
        if tick_size is not None:
            meta["tick_size"] = float(tick_size)

    def update_from_bars(
        self,
        symbol: str,
        df_1m: pd.DataFrame | None = None,
        df_5m: pd.DataFrame | None = None,
        df_1d: pd.DataFrame | None = None,
    ) -> None:
        """Інкрементальне оновлення «книги» рівнів із різних джерел.

        Джерела:
            - Свінги/екстремуми (1d та 5m).
            - Volume Profile HVN (за швидкою гістограмою 1m).
            - Anchored VWAP ± σ (1m, якоримо на вікно df_1m).

        Алгоритм:
            1) Додаємо нові кандидати в книгу рівнів із базовими вагами/шириною band.
            2) Зливаємо близькі рівні за ε (відносна відстань).
            3) Нормалізуємо ваги у [0..1].

        Args:
            symbol: Символ.
            df_1m: 1-хвилинні бари (мін. ~400 рядків для профілю).
            df_5m: 5-хвилинні бари (мін. ~100 рядків для свінгів).
            df_1d: Денні бари (мін. ~20 рядків для свінгів).
        """
        sym = symbol.lower()
        book = self.level_book.setdefault(sym, [])
        meta = self.symbol_meta.get(sym, {})
        atr_pct = float(meta.get("atr_pct", 0.5))  # у %, напр. 0.5 = 0.5%
        # ε для злиття: половина ATR% у відносних частках
        eps_rel = max(1e-12, atr_pct / 2.0 / 100.0)

        def _add_levels(
            vals: list[float], src: str, tf: str, base_score: float, band: float
        ) -> None:
            for v in vals or []:
                try:
                    fv = float(v)
                    if not np.isfinite(fv):
                        continue
                    book.append(
                        Level(
                            value=fv,
                            score=base_score,
                            source=src,
                            tf=tf,
                            band_pct=float(band),
                        )
                    )
                except (
                    Exception
                ):  # broad except: пропускаємо одиничні некоректні рівні без зриву оновлення
                    continue

        # 1) Pivots/екстремуми
        if (
            df_1d is not None
            and len(df_1d) >= 20
            and {"high", "low"}.issubset(df_1d.columns)
        ):
            highs = df_1d["high"].to_numpy(dtype=float, copy=False)
            lows = df_1d["low"].to_numpy(dtype=float, copy=False)
            daily_ext = self._extract_pivots(highs, lows, win=3)
            _add_levels(daily_ext, "pivot", "1d", base_score=1.5, band=atr_pct)

        if (
            df_5m is not None
            and len(df_5m) >= 100
            and {"high", "low"}.issubset(df_5m.columns)
        ):
            highs = df_5m["high"].to_numpy(dtype=float, copy=False)
            lows = df_5m["low"].to_numpy(dtype=float, copy=False)
            intr_ext = self._extract_pivots(highs, lows, win=3)
            _add_levels(intr_ext, "pivot", "5m", base_score=1.3, band=atr_pct * 0.6)

        # 2) Volume Profile (HVN) з 1m
        if (
            df_1m is not None
            and len(df_1m) >= 400
            and {"close"}.issubset(df_1m.columns)
        ):
            hvn = self._volume_nodes(df_1m, bins=50)
            _add_levels(hvn, "volume", "1m", base_score=1.8, band=atr_pct * 0.5)

        # 3) Anchored VWAP ± σ
        if df_1m is not None and {"close", "volume"}.issubset(df_1m.columns):
            vwap, sigma = self._anchored_vwap(df_1m)
            _add_levels([vwap], "vwap", "1m", base_score=1.4, band=atr_pct * 0.4)
            _add_levels(
                [vwap + sigma, vwap - sigma],
                "vwap_band",
                "1m",
                base_score=1.2,
                band=atr_pct * 0.4,
            )

        # Злиття близьких кандидатів і нормалізація ваг
        self._merge_close(sym, eps_rel=eps_rel)
        self._rescale_scores(sym)

    # Допоміжні алгоритми

    def _extract_pivots(
        self, highs: np.ndarray, lows: np.ndarray, win: int = 3
    ) -> list[float]:
        """Витягує прості свінг-екстремуми.

        Args:
            highs: Масив high.
            lows: Масив low.
            win: Напіввікно (розмір локального оточення).

        Returns:
            Масив значень рівнів (float).
        """
        piv: list[float] = []
        n = int(len(highs))
        if n <= (2 * win + 1):
            return piv
        # свінг-high / свінг-low
        for i in range(win, n - win):
            loc = slice(i - win, i + win + 1)
            h = highs[loc]
            l_arr = lows[loc]
            mid_h = highs[i]
            mid_l = lows[i]
            if mid_h == np.max(h):
                piv.append(float(mid_h))
            if mid_l == np.min(l_arr):
                piv.append(float(mid_l))
        return piv

    def _volume_nodes(self, df: pd.DataFrame, bins: int = 50) -> list[float]:
        """Оцінює HVN за зваженою гістограмою ціни (вага = volume).

        Args:
            df: DataFrame 1m із `close` (+ бажано `volume`).
            bins: Кількість бінів.

        Returns:
            Топ-3 вузлів (центри бінів) за вагою обсягу.
        """
        closes = df["close"].to_numpy(dtype=float, copy=False)
        vols = (
            df["volume"].to_numpy(dtype=float, copy=False)
            if "volume" in df.columns
            else np.ones_like(closes)
        )
        if len(closes) < 10:
            return []
        hist, edges = np.histogram(closes, bins=bins, weights=vols)
        if hist.size == 0 or float(hist.max(initial=0.0)) <= 0.0:
            return []
        centers = (edges[:-1] + edges[1:]) / 2.0
        top_idx = np.argsort(hist)[-3:]
        return [float(centers[i]) for i in sorted(top_idx)]

    def _anchored_vwap(self, df: pd.DataFrame) -> tuple[float, float]:
        """Обчислює Anchored VWAP і σ навколо нього (простий якор на вікні df)."""
        p = df["close"].to_numpy(dtype=float, copy=False)
        v = (
            df["volume"].to_numpy(dtype=float, copy=False)
            if "volume" in df.columns
            else np.ones_like(p)
        )
        mask = np.isfinite(p) & np.isfinite(v)
        if not mask.any():
            logger.debug("[VWAP] No valid price/volume data. Ставимо 0.0, 0.0")
            return 0.0, 0.0
        p = p[mask]
        v = v[mask]
        v_sum = float(np.sum(v))
        if v_sum <= 0.0:
            v_sum = 1.0
        vwap = float(np.sum(p * v) / v_sum)
        diffs = p - vwap
        diffs = diffs[np.isfinite(diffs)]
        sigma = float(np.nanstd(diffs)) if diffs.size else 0.0
        return vwap, sigma

    def _merge_close(self, symbol: str, eps_rel: float = 0.002) -> None:
        """Зливає близькі рівні (ε — відносний), зважене середнє за score.

        Args:
            symbol: Символ.
            eps_rel: Відносна відстань для злиття (напр., 0.002 = 0.2%).
        """
        sym = symbol.lower()
        book = self.level_book.get(sym, [])
        if not book:
            return

        # Сортуємо за значенням
        book.sort(key=lambda x: x.value)
        merged: list[Level] = []
        cur = book[0]

        for nxt in book[1:]:
            if not (np.isfinite(cur.value) and np.isfinite(nxt.value)):
                continue
            rel = abs(nxt.value - cur.value) / max(1e-12, cur.value)
            if rel <= eps_rel:
                tot = cur.score + nxt.score
                if tot <= 0:
                    tot = 1.0
                cur = Level(
                    value=(cur.value * cur.score + nxt.value * nxt.score) / tot,
                    score=tot,
                    source="merged",
                    tf=cur.tf,
                    touches=cur.touches + nxt.touches,
                    last_touch_ts=cur.last_touch_ts or nxt.last_touch_ts,
                    band_pct=max(cur.band_pct, nxt.band_pct),
                )
            else:
                merged.append(cur)
                cur = nxt

        merged.append(cur)
        self.level_book[sym] = merged

    def _rescale_scores(self, symbol: str) -> None:
        """Нормалізує ваги рівнів у [0..1] для символу."""
        sym = symbol.lower()
        book = self.level_book.get(sym, [])
        if not book:
            return
        smax = max((lv.score for lv in book), default=1.0)
        if smax <= 0:
            smax = 1.0
        for lv in book:
            lv.score = lv.score / smax

    def _nearest_level_objs(
        self, symbol: str, price: float
    ) -> tuple[Level | None, Level | None]:
        """Повертає найближчі об’єкти рівнів (з урахуванням ваг).

        Ефективна відстань: |Δ| / (1 + score).
        """
        sym = symbol.lower()
        book = self.level_book.get(sym, [])
        if not book:
            return None, None

        below = [lv for lv in book if np.isfinite(lv.value) and lv.value < price]
        above = [lv for lv in book if np.isfinite(lv.value) and lv.value > price]

        def eff_dist(lv: Level) -> float:
            return abs(price - lv.value) / (1.0 + float(lv.score))

        support = min(below, key=eff_dist, default=None)
        resistance = min(above, key=eff_dist, default=None)
        return support, resistance

    def _nearest_by_score(
        self, symbol: str, price: float
    ) -> tuple[float | None, float | None]:
        """Адаптер: повертає значення рівнів (без об’єктів) із урахуванням ваг."""
        s_obj, r_obj = self._nearest_level_objs(symbol, price)
        return (s_obj.value if s_obj else None, r_obj.value if r_obj else None)

    # Додатково: коридор рівнів для контексту/ризику

    def get_corridor(
        self,
        symbol: str,
        price: float,
        daily_low: float | None = None,
        daily_high: float | None = None,
    ) -> dict[str, float | None]:
        """Формує «коридор» рівнів і базову довіру.

        Args:
            symbol: Символ.
            price: Поточна ціна.
            daily_low: Для м'якого seed (опційно).
            daily_high: Для м'якого seed (опційно).

        Returns:
            dict із ключами:
            - support, resistance: найближчі межі (float|None)
            - mid: середина коридора (float|None)
            - band_pct: орієнтовна «ширина» коридора у % (float|None)
            - confidence: базова довіра 0..1 (float|None)
            - dist_to_support_pct, dist_to_resistance_pct: відстані у % (float|None)
        """
        # гарантуємо наявність nearest
        s_val, r_val = self.ensure_and_get_nearest_levels(
            symbol, price, daily_low, daily_high
        )

        # Якщо книга рівнів є — краще дістати об’єкти для band/score
        s_obj, r_obj = (None, None)
        sym = symbol.lower()
        if sym in self.level_book and self.level_book[sym]:
            s_obj, r_obj = self._nearest_level_objs(sym, price)

        support = s_val
        resistance = r_val
        mid = None
        band_pct = None
        confidence = None
        dist_s = None
        dist_r = None

        if support is not None and resistance is not None and resistance > support:
            mid = (support + resistance) / 2.0
            # band: беремо макс локальних band_pct (як %), інакше оцінюємо відносно ширини коридору
            if s_obj and r_obj:
                band_pct = float(max(s_obj.band_pct, r_obj.band_pct))
                confidence = float(
                    min(1.0, max(0.0, 0.5 * (s_obj.score + r_obj.score)))
                )
            else:
                width_rel = abs(resistance - support) / max(1e-12, price)
                band_pct = float(min(5.0, width_rel * 100.0))  # груба оцінка
                confidence = float(
                    max(0.0, 1.0 - width_rel * 2.0)
                )  # вужче → більша довіра

            dist_s = abs(price - support) / max(1e-12, price) * 100.0
            dist_r = abs(resistance - price) / max(1e-12, price) * 100.0

        # ── СИНТЕТИЧНИЙ ФОЛБЕК, якщо бракує меж ─────────────────────────────
        if support is None or resistance is None:
            all_levels = self.get_all_levels(symbol) or []
            ups = [
                lv for lv in all_levels if isinstance(lv, (int, float)) and lv > price
            ]
            dns = [
                lv for lv in all_levels if isinstance(lv, (int, float)) and lv < price
            ]
            meta = self.symbol_meta.get(symbol.lower(), {})
            atr_pct_local = float(meta.get("atr_pct", 0.5)) / 100.0
            if resistance is None:
                resistance = (
                    min(ups) if ups else price * (1.0 + max(atr_pct_local * 1.5, 0.005))
                )
            if support is None:
                support = (
                    max(dns) if dns else price * (1.0 - max(atr_pct_local * 1.5, 0.005))
                )

        # перерахунок мета, якщо з’явилися обидві межі
        if support is not None and resistance is not None and resistance > support:
            mid = (support + resistance) / 2.0
            width_rel = abs(resistance - support) / max(1e-12, price)
            if band_pct is None:
                band_pct = float(min(5.0, width_rel * 100.0))
            if confidence is None:
                confidence = float(max(0.0, 1.0 - width_rel * 2.0))
            dist_s = abs(price - support) / max(1e-12, price) * 100.0
            dist_r = abs(resistance - price) / max(1e-12, price) * 100.0

        return {
            "support": support,
            "resistance": resistance,
            "mid": mid,
            "band_pct": band_pct,
            "confidence": confidence,
            "dist_to_support_pct": dist_s,
            "dist_to_resistance_pct": dist_r,
        }

    def evidence_around(
        self, symbol: str, price_level: float, pct_window: float = 0.12
    ) -> dict[str, int]:
        """
        Повертає кількість рівнів кожного типу в діапазоні ±pct_window% від price_level.
        Використовується для наративу (чи є HVN/VWAP «кластери» поруч).
        """
        sym = (symbol or "").lower()
        book = self.level_book.get(sym, [])
        if not book or not isinstance(price_level, (int, float)):
            return {}
        res: dict[str, int] = {}
        for lv in book:
            if lv.value <= 0:
                continue
            rel = abs(lv.value - price_level) / max(1e-12, price_level)
            if rel <= (pct_window / 100.0):
                res[lv.source] = res.get(lv.source, 0) + 1
        return res
