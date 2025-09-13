# stage1/visualization.py
"""
Візуалізація результатів фільтрації активів Binance USDT-M Futures
------------------------------------------------------------
Цей модуль відповідає за виведення результатів фільтрації у зручному форматі.
Він використовує Rich для форматування таблиць та виведення логів.
Головні функції:
- `print_results`: виводить результати фільтрації у зручному вигляді
- `print_metrics`: виводить метрики фільтрації
- `print_params`: виводить параметри фільтрації
- `print_stages`: виводить етапи фільтрації
"""

import time
import logging

from utils.utils import format_open_interest, format_volume_usd

from rich.table import Table
from rich.box import SIMPLE

from typing import List

from config.config import FilterParams, MetricResults

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


def print_results(result: List[str], metrics: MetricResults):
    """
    Виводить результати фільтрації у зручному форматі.
    :param results: список відфільтрованих символів
    :param metrics: об'єкт з метриками фільтрації
    """

    elapsed = metrics.elapsed_time
    params = FilterParams(**metrics.params)  # Конвертуємо назад у об'єкт FilterParams

    if not result:
        console.print(
            "[bold red]Не знайдено жодного активу за заданими параметрами![/bold red]"
        )
        return

    # Визначаємо ширину для всіх блоків
    block_width = 64

    # Виводимо заголовок без рамки, центрований, білим кольором
    console.print(
        "\n[bold white]{:^64}[/bold white]".format("РЕЗУЛЬТАТ ФІЛЬТРАЦІЇ АКТИВІВ")
    )
    console.print("=" * block_width)

    # Створення таблиці результатів
    result_table = Table(show_header=False, box=None, padding=(0, 1), width=block_width)
    result_table.add_column(style="bold cyan", justify="right", width=30)
    result_table.add_column(style="bold white", justify="left", width=34)

    # Змінюємо рядки, щоб "Активів знайдено" було в одному рядку
    result_table.add_row("Активів знайдено:", f"[bold green]{len(result)}[/bold green]")
    result_table.add_row("Час виконання:", f"[bold]{elapsed:.4f} сек[/bold]")
    result_table.add_row(
        "Топ-5 символів:",
        f"[magenta]{', '.join(result[:5]) if result else '-'}[/magenta]",
    )

    console.print(result_table)
    console.print("-" * block_width)

    # Таблиця параметрів
    param_table = Table(
        title="[bold]ПАРАМЕТРИ ФІЛЬТРАЦІЇ[/bold]",
        box=SIMPLE,
        header_style="bold yellow",
        padding=(0, 2),
        width=block_width,
    )
    param_table.add_column("Параметр", style="cyan", width=30)
    param_table.add_column("Значення", style="white", justify="right", width=34)

    param_table.add_row("min_quote_volume", format_volume_usd(params.min_quote_volume))
    param_table.add_row("min_price_change", f"{params.min_price_change:.2f}%")
    param_table.add_row(
        "min_open_interest", format_open_interest(params.min_open_interest)
    )
    param_table.add_row(
        "min_orderbook_depth", format_volume_usd(params.min_orderbook_depth)
    )
    param_table.add_row("min_atr_percent", f"{params.min_atr_percent:.2f}%")
    param_table.add_row("max_symbols", str(params.max_symbols))
    param_table.add_row("Динамічні пороги", "✅" if params.dynamic else "❌")

    console.print(param_table)
    console.print("-" * block_width)

    # Таблиця етапів фільтрації
    stage_table = Table(
        title="[bold]ЕТАПИ ФІЛЬТРАЦІЇ[/bold]",
        box=SIMPLE,
        header_style="bold yellow",
        padding=(0, 2),
        width=block_width,
    )
    stage_table.add_column("Етап", style="cyan", width=30)
    stage_table.add_column("Кількість", style="white", justify="right", width=34)

    stage_table.add_row("Початковий список", str(metrics.initial_count))
    stage_table.add_row("Після базової фільтрації", str(metrics.prefiltered_count))
    stage_table.add_row("Після повної фільтрації", str(metrics.filtered_count))
    stage_table.add_row(
        "Фінальний результат", f"[bold green]{metrics.result_count}[/bold green]"
    )

    console.print(stage_table)
    console.print("=" * block_width + "\n")
