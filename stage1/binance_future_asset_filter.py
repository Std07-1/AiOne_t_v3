# stage1/binance_future_asset_filter.py

"""
Супершвидкий фільтр USDT‑M‑ф'ючерсів Binance з розширеними метриками
Головні можливості
* Паралельний збір даних з обмеженням семафорів
* Динамічні пороги на основі перцентилів
* Кешування exchangeInfo у Redis (3 год)
* Pydantic валідація параметрів
* Детальне логування та обробка помилок
* Ранжування за комбінованим liquidity_score
* Миттєва обробка до 500+ символів
Вихід: відсортований список тікерів, готовий для подальшої обробки
"""

import logging
import time
from typing import List, Union

import aiohttp
import asyncio
import pandas as pd

from stage1.config import SymbolInfo, FilterParams
from stage1.utils import format_open_interest, format_volume_usd

from stage1.helpers import (
    _fetch_json,
    fetch_cached_data,
    fetch_open_interest,
    fetch_orderbook_depth,
    fetch_atr,
    fetch_concurrently,
)
from stage1.config import (
    OI_SEMAPHORE,
    KLINES_SEMAPHORE,
    DEPTH_SEMAPHORE,
)

from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    MofNCompleteColumn,
    TaskProgressColumn,
)

from stage1.visualization import print_results
from stage1.config import MetricResults

from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("binance_future_asset_filter")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False

# Глобальний консоль для зручності
console = Console()


# CORE LOGIC
class BinanceFutureAssetFilter:
    def __init__(self, session: aiohttp.ClientSession, cache_handler):
        """
        Ініціалізація фільтра активів Binance Futures.
        :param session: aiohttp.ClientSession для HTTP-запитів
        :param cache_handler: обробник кешу (наприклад, Redis)
        """
        self.session = session
        self.cache_handler = cache_handler
        self.metrics = {}
        self.progress = None
        self.metrics_progress = None  # Додатковий атрибут для прогресу метрик
        logger.debug("BinanceFutureAssetFilter ініціалізовано")

    async def load_exchange_info(self) -> List[SymbolInfo]:
        """
        Завантаження інформації про символи з кешу або API Binance.
        Повертає список SymbolInfo для USDT-PERPETUAL TRADING символів.
        """
        logger.debug("[STEP] Початок завантаження exchangeInfo")

        def process_data(data: Union[dict, list]) -> List[dict]:
            # Обробка різних форматів вхідних даних
            logger.debug(f"[EVENT] Обробка exchangeInfo, тип: {type(data)}")
            symbols = data.get("symbols", []) if isinstance(data, dict) else data
            filtered = [
                s
                for s in symbols
                if s.get("quoteAsset") == "USDT"
                and s.get("status") == "TRADING"
                and s.get("contractType") == "PERPETUAL"
            ]
            logger.debug(
                f"[EVENT] Відфільтровано {len(filtered)} символів з exchangeInfo"
            )
            return filtered

        data = await fetch_cached_data(
            self.session,
            self.cache_handler,
            "binance_futures_exchange_info",
            "https://fapi.binance.com/fapi/v1/exchangeInfo",
            process_data,
        )
        logger.debug(f"[STEP] Завантажено exchangeInfo, кількість: {len(data)}")
        return [SymbolInfo(**s) for s in data]

    async def fetch_ticker_data(self) -> pd.DataFrame:
        """
        Отримання даних 24h ticker з Binance Futures API.
        Повертає DataFrame з даними по всіх символах.
        """
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        logger.debug(f"[STEP] Запит ticker/24hr: {url}")
        try:
            data = await _fetch_json(self.session, url)
            logger.debug(f"[EVENT] Отримано {len(data)} записів ticker")
            return pd.DataFrame(data)
        except Exception as e:
            logger.error("Помилка отримання ticker даних: %s", e)
            return pd.DataFrame()

    async def apply_dynamic_thresholds(
        self, df: pd.DataFrame, params: FilterParams
    ) -> FilterParams:
        """
        Розрахунок динамічних порогів для фільтрації активів на основі перцентилів.
        Оновлює params відповідно до статистики по quoteVolume та priceChangePercent.
        """
        logger.debug("[STEP] Початок розрахунку динамічних порогів")
        try:
            df = df.copy()
            df["quoteVolume"] = pd.to_numeric(df["quoteVolume"], errors="coerce")
            df["priceChangePercent"] = pd.to_numeric(
                df["priceChangePercent"], errors="coerce"
            ).abs()
            logger.debug(f"[EVENT] Перетворено типи, кількість записів: {len(df)}")

            df = df.dropna(subset=["quoteVolume", "priceChangePercent"])
            logger.debug(f"[EVENT] Після dropna: {len(df)} записів")

            if len(df) > 10:
                params.min_quote_volume = df["quoteVolume"].quantile(0.75)
                params.min_price_change = df["priceChangePercent"].quantile(0.70)
                logger.info(
                    "Динамічні пороги: Vol ≥ %.2f, Δ%% ≥ %.2f",
                    params.min_quote_volume,
                    params.min_price_change,
                )
                logger.debug(
                    f"[EVENT] Встановлено пороги: min_quote_volume={params.min_quote_volume}, min_price_change={params.min_price_change}"
                )
            else:
                logger.debug("[EVENT] Недостатньо даних для динамічних порогів")
            return params
        except Exception as e:
            logger.error("Помилка розрахунку динамічних порогів: %s", e)
            return params

    async def filter_assets(self, params: FilterParams) -> List[str]:
        """
        Основний пайплайн фільтрації активів Binance Futures.
        Виконує всі етапи: завантаження, фільтрація, збір метрик, ранжування.
        Повертає відсортований список символів.
        """
        start_time = time.monotonic()
        console.print("🔍 [bold cyan]Початок фільтрації активів...[/bold cyan]")

        # Створюємо єдиний прогрес-бар з розширеними налаштуваннями
        self.progress = Progress(
            SpinnerColumn("dots", style="bold cyan"),
            BarColumn(
                bar_width=40,
                complete_style="bold rgb(0,200,0)",
                finished_style="bold green",
                pulse_style="bold yellow",
            ),
            TaskProgressColumn(
                text_format="[bold]{task.percentage:>3.0f}%[/bold]", style="bold white"
            ),
            TextColumn("•", style="dim"),
            MofNCompleteColumn(),
            TextColumn("•", style="dim"),
            TextColumn("[bold]{task.description}", style="bold white"),
            TextColumn("•", style="dim"),
            TimeElapsedColumn(),
            console=Console(stderr=True),
            transient=False,
            refresh_per_second=20,
        )
        self.progress.start()

        # Загальна кількість етапів
        total_steps = 9
        main_task = self.progress.add_task("Фільтрація активів...", total=total_steps)

        # Крок 1: Завантаження базових даних
        logger.debug("[STEP] Крок 1: Завантаження exchangeInfo")
        self.progress.update(main_task, description="[cyan]🔍 Завантаження даних...")
        exchange_info = await self.load_exchange_info()
        valid_symbols = {s.symbol for s in exchange_info}
        logger.debug(f"[EVENT] Завантажено {len(valid_symbols)} валідних символів")

        self.progress.advance(main_task)

        # Завантаження ticker даних
        self.progress.update(
            main_task, description="[cyan]📊 Отримання даних тикера..."
        )
        ticker_df = await self.fetch_ticker_data()
        logger.debug(f"[EVENT] Завантажено ticker_df, shape: {ticker_df.shape}")
        if ticker_df.empty:
            logger.error("Не вдалося отримати дані ticker")
            return []

        self.progress.advance(main_task)  # Просуваємо прогрес

        # Крок 2: Динамічні пороги (якщо активовано)
        self.progress.update(main_task, description="[cyan]⚙️ Розрахунок порогів...")
        logger.debug("[STEP] Крок 2: Динамічні пороги")
        if params.dynamic:
            params = await self.apply_dynamic_thresholds(ticker_df, params)
            logger.debug(f"[EVENT] Параметри після динаміки: {params.dict()}")
        else:
            logger.debug("[EVENT] Динамічні пороги вимкнено, використовуємо статичні")

        self.progress.advance(main_task)

        # Крок 3: Базовий фільтр
        logger.debug("[STEP] Крок 3: Базовий фільтр")
        self.progress.update(main_task, description="[cyan]⚙️ Базова фільтрація...")
        ticker_df = ticker_df[ticker_df["symbol"].isin(valid_symbols)].copy()
        ticker_df["quoteVolume"] = pd.to_numeric(
            ticker_df["quoteVolume"], errors="coerce"
        )
        ticker_df["priceChangePercent"] = pd.to_numeric(
            ticker_df["priceChangePercent"], errors="coerce"
        ).abs()
        logger.debug(f"[EVENT] Після фільтрації по symbol: {ticker_df.shape}")

        base_mask = (ticker_df["quoteVolume"] >= params.min_quote_volume) & (
            ticker_df["priceChangePercent"] >= params.min_price_change
        )
        prefiltered_df = ticker_df[base_mask].copy()
        logger.debug(f"[EVENT] Після базового маску: {prefiltered_df.shape}")

        if prefiltered_df.empty:
            logger.warning("Немає активів після базової фільтрації")
            return []

        symbols = prefiltered_df["symbol"].tolist()
        logger.debug("Після базової фільтрації: %d активів", len(symbols))
        # Логування символів для додаткових метрик
        logger.debug(f"[EVENT] Символи для додаткових метрик: {symbols}")

        self.progress.advance(main_task)

        # Крок 4: Паралельний збір додаткових метрик
        logger.debug("[STEP] Крок 4: Паралельний збір openInterest")
        self.progress.update(main_task, description="[cyan]📈 Збір метрик...")

        # Створюємо завдання для метрик тільки зараз, коли знаємо symbols
        total_metrics = len(symbols) * 3
        metrics_task = self.progress.add_task(
            "[bold yellow] OI • Depth • ATR[/bold yellow]", total=total_metrics
        )

        # Паралельний збір openInterest
        oi_data = await fetch_concurrently(
            self.session,
            symbols,
            fetch_open_interest,
            OI_SEMAPHORE,
            progress_callback=lambda: self.progress.advance(metrics_task),
        )
        logger.debug(f"[EVENT] Зібрано openInterest для {len(oi_data)} символів")

        logger.debug("[STEP] Крок 4: Паралельний збір orderbookDepth")

        # Паралельний збір orderbookDepth
        depth_data = await fetch_concurrently(
            self.session,
            symbols,
            fetch_orderbook_depth,
            DEPTH_SEMAPHORE,
            progress_callback=lambda: self.progress.advance(metrics_task),
        )
        logger.debug(f"[EVENT] Зібрано orderbookDepth для {len(depth_data)} символів")

        logger.debug("[STEP] Крок 4: Паралельний збір ATR")

        # Паралельний збір ATR
        atr_data = await fetch_concurrently(
            self.session,
            symbols,
            fetch_atr,
            KLINES_SEMAPHORE,
            progress_callback=lambda: self.progress.advance(metrics_task),
        )
        logger.debug(f"[EVENT] Зібрано ATR для {len(atr_data)} символів")

        self.progress.advance(main_task)

        # Крок 5: Оновлення DataFrame
        logger.debug("[STEP] Крок 5: Оновлення DataFrame додатковими метриками")

        self.progress.update(main_task, description="Оновлення даних")
        self.progress.update(main_task, description="[cyan]🔄 Оновлення даних...")
        prefiltered_df["openInterest"] = prefiltered_df["symbol"].map(oi_data)
        prefiltered_df["orderbookDepth"] = prefiltered_df["symbol"].map(depth_data)
        prefiltered_df["atrPercent"] = prefiltered_df["symbol"].map(atr_data)
        logger.debug(
            f"[EVENT] DataFrame після додавання метрик: {prefiltered_df.shape}"
        )
        self.progress.advance(main_task)

        # Логування для глибини стакану та open interest
        for sym in prefiltered_df["symbol"]:
            depth_val = prefiltered_df.loc[
                prefiltered_df["symbol"] == sym, "orderbookDepth"
            ].values[0]
            oi_val = prefiltered_df.loc[
                prefiltered_df["symbol"] == sym, "openInterest"
            ].values[0]
            logger.debug(
                f"[EVENT] Глибина стакану для {sym}: {format_volume_usd(depth_val)}"
            )
            logger.debug(
                f"[EVENT] Open Interest для {sym}: {format_open_interest(oi_val)}"
            )
        self.progress.update(main_task, advance=1)

        # Крок 6: Додаткова фільтрація
        logger.debug(
            "[STEP] Крок 6: Додаткова фільтрація по openInterest, orderbookDepth, atrPercent"
        )
        self.progress.update(main_task, description="[cyan]⚙️ Додаткова фільтрація...")
        filtered_df = prefiltered_df[
            (prefiltered_df["openInterest"] >= params.min_open_interest)
            & (prefiltered_df["orderbookDepth"] >= params.min_orderbook_depth)
            & (prefiltered_df["atrPercent"] >= params.min_atr_percent)
        ].copy()
        logger.debug(f"[EVENT] Після додаткової фільтрації: {filtered_df.shape}")

        if filtered_df.empty:
            logger.warning("Немає активів після повної фільтрації")
            return []
        self.progress.advance(main_task)

        # Крок 7: Ранжування активів
        logger.debug("[STEP] Крок 7: Ранжування активів")
        self.progress.update(main_task, description="[cyan]🏆 Ранжування активів...")
        filtered_df["liquidity_score"] = (
            0.5 * filtered_df["quoteVolume"] / filtered_df["quoteVolume"].max()
            + 0.3 * filtered_df["openInterest"] / filtered_df["openInterest"].max()
            + 0.2 * filtered_df["orderbookDepth"] / filtered_df["orderbookDepth"].max()
        )
        logger.debug(f"[EVENT] Додано liquidity_score, shape: {filtered_df.shape}")

        result = (
            filtered_df.sort_values("liquidity_score", ascending=False)
            .head(params.max_symbols)["symbol"]
            .tolist()
        )
        logger.debug(f"[EVENT] Відсортовано фінальний список: {result}")
        self.progress.advance(main_task)

        # Завершення
        self.progress.update(
            main_task, description="[bold green]✅ Завершено![/bold green]"
        )

        # Анімація завершення
        for _ in range(3):
            self.progress.update(
                main_task,
                description="[blink bold green]🚀 Успішно завершено![/blink bold green]",
            )
            await asyncio.sleep(0.2)
        self.progress.stop()

        # Крок 8: Обробка результатів
        elapsed = time.monotonic() - start_time

        # Створюємо об'єкт метрик
        metrics = MetricResults(
            initial_count=len(ticker_df),
            prefiltered_count=len(prefiltered_df),
            filtered_count=len(filtered_df),
            result_count=len(result),
            elapsed_time=elapsed,
            params=params.dict(),
        )

        # Виводимо результати
        print_results(result, metrics)

        # Зберігаємо метрики для налагодження
        self.metrics = metrics
        logger.debug(f"[EVENT] Збережено метрики: {self.metrics}")

        return result
