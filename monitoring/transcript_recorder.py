"""Transcript Recorder — режим стенограми для реального часу.

Записує події (bars, signals, outcomes) у JSONL для подальшого аналізу:
- bar: закриті 1m бари зі стріму
- signal: результати Stage1 (ALERT/NORMAL) з причинами/метриками
- outcome: виміряний рух ціни через N секунд після сигналу

Увімкнення: змінна середовища MONITOR_TRANSCRIPT=1 (ініціалізація у app.main).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class TranscriptConfig:
    base_dir: str
    # Де зберігати файли, відносно base_dir
    subdir: str = "transcripts"
    # Горизонти для оцінки наслідків сигналів (секунди)
    outcome_horizons_s: list[int] = field(default_factory=lambda: [60, 300, 900])
    # Лінивий флеш: після N записів примусово flush
    flush_every: int = 100
    # Додатково: періодичний flush раз на N секунд (щоб не залишались пусті файли)
    flush_interval_s: float = 5.0
    # Обмеження запису за символами (None — без обмежень)
    allowed_symbols: set[str] | None = None


class TranscriptRecorder:
    """Асинхронний JSONL‑записувач стенограми подій.

    Методи log_* неконсуптивні та швидкі: події додаються до черги.
    Письмо у файл виконує окремий воркер.
    """

    def __init__(self, cfg: TranscriptConfig) -> None:
        self.cfg = cfg
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=10000)
        self._task: asyncio.Task[Any] | None = None
        # Формуємо шлях до файла
        ts = time.strftime("%Y%m%d_%H%M%S")
        out_dir = Path(cfg.base_dir) / cfg.subdir
        out_dir.mkdir(parents=True, exist_ok=True)
        self._path = out_dir / f"transcript_{ts}.jsonl"
        self._lines_written = 0
        self._stopping = asyncio.Event()
        self._last_flush = time.monotonic()
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._writer())

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            try:
                await self._queue.put("__STOP__\n")
            except Exception:
                pass
            try:
                await self._task
            finally:
                self._task = None

    async def _writer(self) -> None:
        # Відкриваємо файл у текстовому режимі (UTF‑8)
        # На Windows бажано уникати редагування в реальному часі іншими процесами
        with open(self._path, "a", encoding="utf-8") as f:
            while not self._stopping.is_set():
                line: str | None = None
                try:
                    # Чекаємо або нову подію, або таймаут для періодичного flush
                    line = await asyncio.wait_for(
                        self._queue.get(), timeout=max(0.5, self.cfg.flush_interval_s)
                    )
                except TimeoutError:
                    # Немає нових подій — зробимо часовий flush, якщо треба
                    pass

                if line is not None:
                    if line == "__STOP__\n":
                        try:
                            f.flush()
                            os.fsync(f.fileno())
                        except Exception:
                            pass
                        break
                    f.write(line)
                    self._lines_written += 1

                # Перевірка умов для flush: по лічильнику або за часом
                do_flush = False
                if self._lines_written > 0 and (
                    self._lines_written % max(1, self.cfg.flush_every) == 0
                ):
                    do_flush = True
                else:
                    now = time.monotonic()
                    if now - self._last_flush >= max(0.5, self.cfg.flush_interval_s):
                        do_flush = True

                if do_flush:
                    try:
                        f.flush()
                        os.fsync(f.fileno())
                        self._last_flush = time.monotonic()
                    except Exception:
                        pass

    def _enqueue(self, obj: dict[str, Any]) -> None:
        try:
            line = json.dumps(obj, ensure_ascii=False) + "\n"
            # не await — черга асинхронна, але put_nowait швидший
            self._queue.put_nowait(line)
        except asyncio.QueueFull:
            # дропимо подію, щоб не блокувати критичні шляхи
            try:
                self._logger.debug(
                    "Transcript queue full → drop", extra={"head": 3, "tail": 3}
                )
            except Exception:
                pass

    # ── Події ──
    def log_bar(
        self,
        *,
        symbol: str,
        interval: str,
        ts_ms: int,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        source: str = "ws",
        closed: bool = True,
    ) -> None:
        if (
            self.cfg.allowed_symbols is not None
            and symbol not in self.cfg.allowed_symbols
        ):
            return
        self._enqueue(
            {
                "type": "bar",
                "symbol": symbol,
                "interval": interval,
                "ts": int(ts_ms),
                "open": float(open),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "volume": float(volume),
                "source": source,
                "closed": bool(closed),
            }
        )

    # ── Службові події ──
    def log_meta(self, **fields: Any) -> None:
        """Лог службової події (метадані, діагностика)."""
        payload: dict[str, Any] = {"type": "meta"}
        payload.update(fields)
        self._enqueue(payload)

    def log_signal(
        self,
        *,
        symbol: str,
        ts_ms: int,
        price: float,
        signal: str,
        reasons: list[str] | None = None,
        stats: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> str:
        if (
            self.cfg.allowed_symbols is not None
            and symbol not in self.cfg.allowed_symbols
        ):
            return ""
        sid = str(uuid.uuid4())
        payload: dict[str, Any] = {
            "type": "signal",
            "id": sid,
            "symbol": symbol,
            "ts": int(ts_ms),
            "price": float(price),
            "signal": str(signal),
            "reasons": list(reasons or []),
            "reasons_count": int(len(reasons or [])),
        }
        if stats:
            # обрізаємо зайве: беремо ключові поля
            keep = (
                "rsi",
                "volume_z",
                "atr",
                "vwap",
                "dynamic_overbought",
                "dynamic_oversold",
            )
            payload["stats"] = {k: stats.get(k) for k in keep if k in stats}
        if extra:
            payload["extra"] = dict(extra)
        self._enqueue(payload)
        return sid

    def schedule_outcomes(
        self,
        *,
        store: Any,
        symbol: str,
        signal_id: str,
        base_ts_ms: int,
        base_price: float,
        horizons_s: list[int] | None = None,
    ) -> None:
        """Планує обчислення результатів через задані горизонти.

        Використовує UnifiedDataStore.get_df для отримання актуальних барів
        та оцінює відхилення ціни від базової.
        """
        hs = horizons_s or self.cfg.outcome_horizons_s
        for h in hs:
            asyncio.create_task(
                self._compute_outcome(
                    store, symbol, signal_id, base_ts_ms, base_price, h
                )
            )

    async def _compute_outcome(
        self,
        store: Any,
        symbol: str,
        signal_id: str,
        base_ts_ms: int,
        base_price: float,
        horizon_s: int,
    ) -> None:
        try:
            # Чекаємо реальний час
            await asyncio.sleep(max(1, int(horizon_s)))
            # Забираємо останні ~600 барів і шукаємо найближчий до базового ts+h
            df = await store.get_df(symbol, "1m", limit=600)
            if df is None or df.empty:
                return
            if "open_time" in df.columns:
                ts_col = df["open_time"].astype("int64")
            else:
                ts_col = (df["timestamp"].astype("int64") // 1_000_000).astype("int64")
            target_ms = base_ts_ms + horizon_s * 1000
            # знайдемо перший бар із open_time >= target_ms
            idx = (
                int((ts_col >= target_ms).idxmax())
                if (ts_col >= target_ms).any()
                else len(ts_col) - 1
            )
            idx = max(0, min(idx, len(df) - 1))
            row = df.iloc[idx]
            price = float(row.get("close", row.get("c", base_price)))
            delta = (price / base_price - 1.0) if base_price else 0.0
            self._enqueue(
                {
                    "type": "outcome",
                    "ref_id": signal_id,
                    "symbol": symbol,
                    "ts": int(target_ms),
                    "horizon_s": int(horizon_s),
                    "base_price": float(base_price),
                    "price": float(price),
                    "delta": float(delta),
                }
            )
        except Exception:
            # безпека: outcomes не повинні ламати основний потік
            pass
