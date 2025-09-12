# data/cache_handler.py
# -*- coding: utf-8 -*-
"""
Universal async Redis client for AiOne_t
========================================

✔ Works with both local Redis and Heroku (`rediss://` + self-signed TLS).
✔ Built-in retry with exponential backoff & jitter on network errors.
✔ Safe connection-pool cap (MAX_CONNECTIONS = 18, Heroku Mini limit).
✔ Rich DEBUG-logging with head:3\tail:3 data snapshots.
✔ Helpers: ping, close, set_json, get_json.
✔ 100% compatible with redis-py 4.5.5 – no bleeding-edge params.
✔ NEW 2025-04-20
    • raw=True → store/fetch binary blobs (e.g. LZ4-compressed DataFrame).
    • Pipeline helper: store → publish in a single RTT.
    • Per-write status-logging (VALID / NO_DATA).

Typical failures handled automatically:
--------------------------------------
* ConnectionResetError, TimeoutError, BusyLoadingError,
  Heroku "Too many connections", TLS handshake issues, etc.
"""

import asyncio
import json
import logging
import os
import random
import sys
from functools import wraps
from typing import Any, Optional, List, Union

import pandas as pd
from redis.asyncio import Redis
from redis.exceptions import (
    AuthenticationError,
    BusyLoadingError,
    ConnectionError as RedisConnectionError,
    RedisError,
    ResponseError,
    TimeoutError as RedisTimeoutError,
)
from rich.console import Console
from rich.logging import RichHandler
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

# ───────────────────────────── Logging ──────────────────────────────
# --- Логування ---
logger = logging.getLogger("cache_handler")
logger.setLevel(logging.WARNING)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False

# ────────────────────────── Configuration ───────────────────────────
DEFAULT_TTL = 60 * 60  # 1 hour
MAX_RETRIES = 3
BASE_DELAY = 0.4  # initial back-off seconds
MAX_CONNECTIONS = 18  # safe pool size
HEALTH_CHECK_SEC = 30
_REDIS_SEM = asyncio.Semaphore(MAX_CONNECTIONS - 2)  # limit concurrency
CACHE_STATUS_VALID = "VALID"
CACHE_STATUS_NODATA = "NO_DATA"


# ╭──────────────────────── retry decorator ─────────────────────╮
def with_retry(func):
    """
    Async-retry + connection-pool limiter.

    • Limits concurrent ops via `_REDIS_SEM` (avoids too many connections).
    • Retries up to MAX_RETRIES with exponential backoff + jitter.
    """
    retriable = (
        ConnectionError,
        TimeoutError,
        ConnectionResetError,
        RedisConnectionError,
        RedisTimeoutError,
    )

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        delay = BASE_DELAY
        for attempt in range(1, MAX_RETRIES + 2):
            try:
                async with _REDIS_SEM:
                    return await func(self, *args, **kwargs)
            except retriable as exc:
                logger.warning(
                    "[Redis][RETRY %d/%d] %s → %s",
                    attempt,
                    MAX_RETRIES,
                    func.__name__,
                    exc,
                )
                # drop busy connections
                try:
                    await self.client.connection_pool.disconnect(inuse_connections=True)
                except Exception:
                    pass
                if attempt > MAX_RETRIES:
                    raise
                await asyncio.sleep(delay + random.uniform(0, delay / 4))
                delay *= 2

    return wrapper


# ╰──────────────────────────────────────────────────────────────╯


class SimpleCacheHandler:
    """
    Async Redis wrapper (singleton per process).

    Parameters
    ----------
    redis_url : str | None
        Full connection URI or reads REDIS_URL env.
    host : str | None, port : int | None, db : int
        Convenience for local Redis (ignored if redis_url provided).
    """

    FAST_SYMBOLS_KEY = "ai_one:fast_symbols"

    def __init__(
        self,
        redis_url: Optional[str] = None,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: int = 0,
    ) -> None:
        self.redis_url = (
            redis_url
            or os.getenv("REDIS_URL")
            or f"redis://{host or 'localhost'}:{port or 6379}/{db}"
        )
        self.client: Redis = self._create_client(self.redis_url)

    @staticmethod
    def _tls_friendly(url: str) -> str:
        """Injects ssl_cert_reqs=none & ssl_check_hostname=false for rediss://"""
        parsed = urlparse(url)
        if parsed.scheme != "rediss":
            return url
        qs = parse_qs(parsed.query, keep_blank_values=True)
        qs.setdefault("ssl_cert_reqs", ["none"])
        qs.setdefault("ssl_check_hostname", ["false"])
        return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))

    def _create_client(self, url: str) -> Redis:
        url = self._tls_friendly(url)
        parsed = urlparse(url)
        try:
            cli = Redis.from_url(
                url,
                decode_responses=False,  # binary-safe
                health_check_interval=HEALTH_CHECK_SEC,
                socket_keepalive=True,
                max_connections=MAX_CONNECTIONS,
                retry_on_error=[RedisConnectionError, RedisTimeoutError],
            )
            logger.debug(
                "[Redis][INIT] %s://%s:%s  TLS=%s  pool=%d",
                parsed.scheme,
                parsed.hostname,
                parsed.port,
                parsed.scheme == "rediss",
                MAX_CONNECTIONS,
            )
            return cli
        except (AuthenticationError, ResponseError) as exc:
            logger.critical("[Redis][AUTH] %s", exc)
            sys.exit(1)
        except BusyLoadingError as exc:
            logger.error("[Redis][BUSY] %s", exc)
            raise
        except RedisError as exc:
            logger.exception("[Redis][INIT] %s", exc)
            raise

    @staticmethod
    def _key(symbol: str, interval: str, prefix: Optional[str]) -> str:
        return f"{prefix}:{symbol}:{interval}" if prefix else f"{symbol}:{interval}"

    @with_retry
    async def store_in_cache(
        self,
        symbol: str,
        interval: str,
        data: Union[bytes, str],
        *,
        ttl: int = DEFAULT_TTL,
        prefix: Optional[str] = None,
        raw: bool = False,
        publish_channel: Optional[str] = None,
        publish_message: Optional[str] = None,
    ) -> None:
        """
        SETEX key value TTL + optional PUB/SUB publish.
        data: bytes or JSON-serializable str.
        """
        key = self._key(symbol, interval, prefix)
        val = data if isinstance(data, bytes) else str(data).encode()
        pipe = self.client.pipeline()
        pipe.setex(key, ttl, val)
        if publish_channel:
            msg = (publish_message or symbol).encode()
            pipe.publish(publish_channel, msg)
        await pipe.execute()
        logger.debug(
            "[Redis][SET] %s %s(%ds) size=%dB",
            key,
            CACHE_STATUS_VALID,
            ttl,
            len(val),
        )

    @with_retry
    async def fetch_from_cache(
        self,
        symbol: str,
        interval: str,
        *,
        prefix: Optional[str] = None,
        raw: bool = False,
    ) -> Optional[Union[bytes, pd.DataFrame, dict]]:
        """
        GET key → bytes|DataFrame|dict.
        raw=True returns bytes.
        """
        key = self._key(symbol, interval, prefix)
        val: Optional[bytes] = await self.client.get(key)
        ttl = await self.client.ttl(key)
        status = CACHE_STATUS_VALID if val else CACHE_STATUS_NODATA
        logger.debug("[Redis][GET] %s → %s ttl=%ss", key, status, ttl)
        if val is None:
            return None
        if raw:
            return val
        try:
            decoded = json.loads(val.decode())
        except Exception as e:
            logger.error("[Redis][DECODE] %s → %s", key, e)
            return None
        if isinstance(decoded, list):
            df = pd.DataFrame(decoded)
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return df
        return decoded

    @with_retry
    async def delete_from_cache(
        self,
        symbol: str,
        interval: str,
        *,
        prefix: Optional[str] = None,
    ) -> bool:
        key = self._key(symbol, interval, prefix)
        res = await self.client.delete(key)
        logger.debug("[Redis][DEL] %s → %s", key, bool(res))
        return bool(res)

    @with_retry
    async def is_key_exists(
        self,
        symbol: str,
        interval: str,
        *,
        prefix: Optional[str] = None,
    ) -> bool:
        key = self._key(symbol, interval, prefix)
        res = await self.client.exists(key)
        logger.debug("[Redis][EXISTS] %s → %s", key, bool(res))
        return bool(res)

    @with_retry
    async def get_remaining_ttl(
        self,
        symbol: str,
        interval: str,
        *,
        prefix: Optional[str] = None,
    ) -> int:
        key = self._key(symbol, interval, prefix)
        ttl = await self.client.ttl(key)
        logger.debug("[Redis][TTL] %s → %s", key, ttl)
        return ttl

    async def set_json(
        self,
        symbol: str,
        interval: str,
        obj: Any,
        *,
        ttl: int = DEFAULT_TTL,
        prefix: Optional[str] = None,
        ensure_ascii: bool = False,
    ) -> None:
        """Convenience: JSON-dumps + store."""
        await self.store_in_cache(
            symbol,
            interval,
            json.dumps(obj, ensure_ascii=ensure_ascii),
            ttl=ttl,
            prefix=prefix,
            raw=False,
        )

    async def get_json(
        self,
        symbol: str,
        interval: str,
        *,
        prefix: Optional[str] = None,
    ) -> Optional[Any]:
        """Convenience: fetch + return dict|list or None."""
        data = await self.fetch_from_cache(symbol, interval, prefix=prefix, raw=False)
        if isinstance(data, (dict, list)):
            return data
        logger.warning("[Redis][GET_JSON] Unexpected type: %s", type(data))
        return None

    @with_retry
    async def ping(self) -> bool:
        """Health check: PING."""
        res = await self.client.ping()
        logger.debug("[Redis][PING] → %s", res)
        return bool(res)

    async def close(self) -> None:
        """Close connection pool."""
        await self.client.close()
        await self.client.connection_pool.disconnect(inuse_connections=True)
        logger.debug("[Redis][CLOSE] Pool shut down.")

    async def set_fast_symbols(
        self, symbols: list[str], *, ttl: int = DEFAULT_TTL
    ) -> None:
        """
        Записує список string-символів у Redis як JSON (унікальний ключ).
        """
        if not isinstance(symbols, list) or not all(
            isinstance(s, str) for s in symbols
        ):
            raise ValueError("set_fast_symbols: symbols має бути списком рядків!")
        val = json.dumps(symbols, ensure_ascii=False)
        await self.client.set(self.FAST_SYMBOLS_KEY, val, ex=ttl)
        logger.debug(
            "[Redis][SET_FAST_SYMBOLS] %s → %s", self.FAST_SYMBOLS_KEY, symbols
        )

    async def get_fast_symbols(self) -> list[str]:
        """
        Повертає список символів із Redis або [].
        """
        val = await self.client.get(self.FAST_SYMBOLS_KEY)
        if not val:
            logger.debug(
                "[Redis][GET_FAST_SYMBOLS] Ключ %s відсутній!", self.FAST_SYMBOLS_KEY
            )
            return []
        try:
            symbols = json.loads(val.decode() if isinstance(val, bytes) else val)
        except Exception as e:
            logger.warning("[Redis][GET_FAST_SYMBOLS] decode error: %s", e)
            return []
        if isinstance(symbols, list) and all(isinstance(s, str) for s in symbols):
            logger.debug(
                "[Redis][GET_FAST_SYMBOLS] %s → %s", self.FAST_SYMBOLS_KEY, symbols
            )
            return symbols
        logger.warning("[Redis][GET_FAST_SYMBOLS] Unexpected type: %r", type(symbols))
        return []
