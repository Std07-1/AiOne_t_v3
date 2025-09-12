import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Union, Optional
import pandas as pd
from redis.asyncio import Redis
from .utils import serialize_to_json

logger = logging.getLogger("file_manager")
logger.setLevel(logging.WARNING)


class FileManager:
    def __init__(self, redis_client: Optional[Redis] = None, base_dir: str = "symbols"):
        """
        Клас FileManager відповідає за роботу з файлами:
          - зберігання даних (JSON/CSV),
          - читання даних,
          - кешування через Redis (опційно).

        Args:
            redis_client (Optional[Redis]): Асинхронний Redis-клієнт (якщо передано).
            base_dir (str): Базова директорія, в якій створюються папки для символів.
        """
        self.redis_client = redis_client
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_symbol_dir(self, symbol: str) -> str:
        """
        Повертає шлях до папки символу та створює її, якщо вона не існує.
        Наприклад, 'symbols/BTCUSDT'.

        Args:
            symbol (str): Назва активу (символу).

        Returns:
            str: Шлях до папки для цього символу.
        """
        # Якщо символ передано як список, об'єднуємо елементи в рядок
        if isinstance(symbol, list):
            symbol = "_".join(symbol)
        path = os.path.join(self.base_dir, symbol)
        os.makedirs(path, exist_ok=True)
        return path

    def _generate_file_path(
        self, symbol: str, context: str, event_type: str, extension: str = "json"
    ) -> str:
        """
        Генерує ШТАТНИЙ шлях для зберігання/читання файлів
        без додавання часу (TIMESTAMP). Файл щоразу перезаписується
        (або оновлюється).

        Формат файлу:
            {symbol}_{context}_{event_type}.{extension}

        Args:
            symbol (str): Назва символу (наприклад, 'BTCUSDT').
            context (str): Контекст (наприклад, 'thresholds', 'prepared', 'data' тощо).
            event_type (str): Тип події або файлу (наприклад, 'data', 'result').
            extension (str): Розширення ('json' або 'csv').

        Returns:
            str: Повний шлях до файлу.
        """
        dir_path = self._get_symbol_dir(symbol)
        filename = f"{symbol}_{context}_{event_type}.{extension}"
        return os.path.join(dir_path, filename)

    def _format_timestamp(self, x: Any) -> str:
        """
        Форматує значення часу: якщо не вдається перетворити – повертає порожній рядок.
        """
        try:
            if isinstance(x, (int, float)):
                ts = pd.to_datetime(x, unit="ms", errors="coerce")
            else:
                ts = pd.to_datetime(x, errors="coerce")
            if pd.isna(ts):
                return ""
            return ts.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.error(f"Помилка форматування timestamp: {e}", exc_info=True)
            return ""
        
    def save_file(
        self,
        symbol: str,
        event_type: str,
        context: str,
        data: Union[Dict[str, Any], pd.DataFrame],
        extension: str = "json",
    ) -> None:
        """
        Зберігає дані у файл (оновлюючи його), а не створюючи новий
        при кожному виклику.

        Args:
            symbol (str): Назва символу.
            context (str): Контекст (наприклад, 'thresholds', 'prepared').
            event_type (str): Тип події або файлу (наприклад, 'data', 'result').
            data (Dict[str, Any] | pd.DataFrame): Дані для збереження.
            extension (str): Формат збереження: 'json' або 'csv'.
        """
        try:
            file_path = self._generate_file_path(symbol, event_type, context, extension)
            logger.debug(f"[FileManager] Готуємося зберегти файл: {file_path}")

            if extension == "json":
                self._save_json(file_path, data)
            elif extension == "csv":
                self._save_csv(file_path, data)
            else:
                raise ValueError(f"Unsupported file extension: {extension}")

            logger.info(f"[FileManager] Дані збережено у файл: {file_path}")
        except Exception as e:
            logger.error(f"[FileManager] Помилка збереження у файл: {e}", exc_info=True)

    def _save_json(
        self, file_path: str, data: Union[Dict[str, Any], pd.DataFrame]
    ) -> None:
        # Якщо це словник:
        if isinstance(data, dict):
            temp_dict = dict(data)
            if "timestamp" in temp_dict:
                temp_dict["readable_timestamp"] = self._format_timestamp(temp_dict["timestamp"])
            if "close_time" in temp_dict:
                temp_dict["readable_close_time"] = self._format_timestamp(temp_dict["close_time"])
            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(temp_dict, file, indent=4, ensure_ascii=False)
        # Якщо це DataFrame:
        elif isinstance(data, pd.DataFrame):
            temp_df = data.copy()
            if "timestamp" in temp_df.columns:
                temp_df["readable_timestamp"] = temp_df["timestamp"].apply(self._format_timestamp)
            if "close_time" in temp_df.columns:
                temp_df["readable_close_time"] = temp_df["close_time"].apply(self._format_timestamp)
            temp_df.to_json(file_path, orient="records", indent=4, force_ascii=False)
        else:
            raise TypeError(f"Unsupported data type for JSON: {type(data)}")
        
    def _save_csv(self, file_path: str, data: pd.DataFrame) -> None:
        """
        Зберігає DataFrame у форматі CSV.
        """
        if not isinstance(data, pd.DataFrame):
            raise TypeError("Для CSV потрібен DataFrame.")

        temp_df = data.copy()

        # Перетворюємо timestamp і close_time у читаємий формат
        if "timestamp" in temp_df.columns:
            temp_df["readable_timestamp"] = pd.to_datetime(
                temp_df["timestamp"], unit="ms", errors="coerce"
            ).dt.strftime("%Y-%m-%d %H:%M:%S")

        if "close_time" in temp_df.columns:
            temp_df["readable_close_time"] = pd.to_datetime(
                temp_df["close_time"], unit="ms", errors="coerce"
            ).dt.strftime("%Y-%m-%d %H:%M:%S")

        temp_df.to_csv(file_path, index=False)

    def load_file(
        self, symbol: str, event_type: str, context: str, extension: str = "json"
    ) -> Optional[Any]:
        """
        Завантажує файл (оновлюваний), який був сформований під час save_file.
        Очікує той самий шлях без TIMESTAMP.

        Args:
            symbol (str): Назва символу.
            context (str): Контекст (наприклад, 'thresholds', 'prepared').
            event_type (str): Тип події (наприклад, 'data', 'result').
            extension (str): 'json' або 'csv'.

        Returns:
            Optional[Any]: Дані з файлу (dict / list / DataFrame) або None, якщо файл не знайдено чи помилка.
        """
        try:
            file_path = self._generate_file_path(symbol, event_type, context, extension)

            if not os.path.exists(file_path):
                logger.warning(f"[FileManager] Файл {file_path} не знайдено.")
                return None

            if extension == "json":
                return self._load_json(file_path)
            elif extension == "csv":
                return self._load_csv(file_path)
            else:
                raise ValueError(f"Unsupported file extension: {extension}")
        except Exception as e:
            logger.error(
                f"[FileManager] Помилка завантаження файлу: {e}", exc_info=True
            )
            return None

    def _load_json(self, file_path: str) -> Union[Dict[str, Any], pd.DataFrame]:
        """
        Завантажує дані з JSON-файлу.
        """
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        if isinstance(data, list) and all(isinstance(x, dict) for x in data):
            return pd.DataFrame(data)
        return data

    def _load_csv(self, file_path: str) -> pd.DataFrame:
        """
        Завантажує дані з CSV-файлу у DataFrame.
        """
        return pd.read_csv(file_path)

    async def cache_data(
        self,
        symbol: str,
        event_type: str,
        context: str,
        data: Union[Dict[str, Any], pd.DataFrame],
        ttl: int = 3600,
    ) -> None:
        """
        Асинхронне кешування даних у Redis (якщо redis_client заданий),
        якщо Redis недоступний — зберігає у файл (оновлюючи його).

        Args:
            symbol (str):
            context (str):
            event_type (str):
            data (dict / DataFrame):
            ttl (int): Час життя (сек).
        """
        cache_key = f"symbols:{symbol}:{event_type}:{context}"
        try:
            if self.redis_client:
                to_store = (
                    serialize_to_json(data)
                    if isinstance(data, pd.DataFrame)
                    else json.dumps(data)
                )
                await self.redis_client.setex(cache_key, ttl, to_store)
                logger.info(
                    f"[Redis] Дані кешовано. Ключ: {cache_key}, TTL: {ttl} секунд."
                )
            else:
                raise ConnectionError(
                    "Redis недоступний. Використовується файлова система."
                )
        except Exception as e:
            logger.warning(f"[Redis] Помилка кешування: {e}. Дані зберігаються у файл.")
            self.save_file(symbol, event_type, context, data)

    async def get_cached_data(
        self, symbol: str, event_type: str, context: str, extension: str = "json"
    ) -> Optional[Any]:
        """
        Асинхронне отримання даних із Redis (якщо доступний), інакше з файлу (оновлюваного).

        Args:
            symbol (str):
            context (str):
            event_type (str):
            extension (str): 'json' або 'csv'.

        Returns:
            Дані (dict / list / DataFrame) або None, якщо немає в Redis і немає у файлі.
        """
        cache_key = f"symbols:{symbol}:{event_type}:{context}"

        try:
            if self.redis_client:
                cached_data = await self.redis_client.get(cache_key)
                if cached_data:
                    return (
                        json.loads(cached_data)
                        if extension == "json"
                        else pd.read_json(cached_data)
                    )
            logger.info(f"[Redis] Дані за ключем {cache_key} не знайдено.")
        except Exception as e:
            logger.warning(f"[Redis] Помилка доступу: {e}. Пошук у файлі.")

        return self.load_file(symbol, context, event_type, extension)

    async def cache_event_data(
        self,
        symbol: str,
        event_type: str,
        context: str,
        data: Any,
        ttl: Optional[int] = 3600,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Аналогічний метод для кешування "подій" чи "сигналів",
        безпосередньо викликає `cache_data`.

        Args:
            symbol (str):
            context (str):
            event_type (str):
            data (Any):
            ttl (Optional[int]):
            config (Optional[Dict[str, Any]]): Не використовується тут, але може бути.
        """
        try:
            logger.info(
                f"[cache_event_data] Початок кешування для {symbol}, {event_type}, {context}. TTL={ttl}"
            )
            await self.cache_data(symbol, event_type, context, data, ttl=ttl)
        except Exception as e:
            logger.error(
                f"[cache_event_data] Помилка кешування у Redis: {e}. Зберігаємо у файл.",
                exc_info=True,
            )
            self.save_file(symbol, event_type, context, data)
