"""Shared utilities for the dataSource loaders: logging, timing, retries,
and input validation. Engine modules use these instead of duplicating the
same plumbing per backend.
"""

from __future__ import annotations

import functools
import logging
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Optional, Tuple, Type, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def get_logger(name: str) -> logging.Logger:
    """Return a logger namespaced under the ``dataSource`` hierarchy.

    Args:
        name: Suffix for the logger, typically the module or engine name.

    Returns:
        A :class:`logging.Logger` named ``dataSource.<name>``.

    Example:
        >>> logger = get_logger("athena")
        >>> logger.name
        'dataSource.athena'
    """
    return logging.getLogger(f"dataSource.{name}")


@contextmanager
def log_timing(logger: logging.Logger, operation: str) -> Iterator[Dict[str, Any]]:
    """Context manager that logs the wall-clock duration of an operation.

    The yielded dict can be populated with a ``rows`` key before the block
    exits; it is appended to the log line when present. Failures inside the
    block are logged at ERROR level and re-raised unchanged.

    Args:
        logger: Logger the timing line is written to.
        operation: Human-readable operation label (e.g. ``"athena.read_sql"``).

    Yields:
        A mutable dict for optional result stats (``{"rows": int}``).

    Example:
        >>> with log_timing(logger, "duckdb.read_table") as stats:
        ...     df = fetch()
        ...     stats["rows"] = len(df)
    """
    stats: Dict[str, Any] = {}
    start = time.perf_counter()
    try:
        yield stats
    except Exception as exc:
        logger.error(
            "%s failed after %.2fs: %s", operation, time.perf_counter() - start, exc
        )
        raise
    elapsed = time.perf_counter() - start
    rows = stats.get("rows")
    suffix = f" ({rows} rows)" if rows is not None else ""
    logger.info("%s finished in %.2fs%s", operation, elapsed, suffix)


def with_retries(
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    retry_if: Optional[Callable[[BaseException], bool]] = None,
    logger: Optional[logging.Logger] = None,
) -> Callable[[F], F]:
    """Decorator that retries a callable with exponential backoff.

    Args:
        attempts: Total number of attempts (first call included). Must be >= 1.
        base_delay: Seconds to sleep before the first retry.
        backoff: Multiplier applied to the delay after each retry.
        exceptions: Exception types eligible for a retry.
        retry_if: Optional predicate; when provided, a matching exception is
            only retried if ``retry_if(exc)`` is truthy (e.g. throttling only).
        logger: Optional logger; retries are logged at WARNING level.

    Returns:
        A decorator wrapping the callable with retry behaviour.

    Raises:
        ValueError: If ``attempts`` is less than 1.

    Example:
        >>> fetch = with_retries(attempts=3, base_delay=0.5)(fetch_once)
        >>> data = fetch()
    """
    if attempts < 1:
        raise ValueError("`attempts` must be >= 1.")

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = base_delay
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    should_retry = retry_if is None or retry_if(exc)
                    if attempt == attempts or not should_retry:
                        raise
                    if logger is not None:
                        logger.warning(
                            "%s failed (attempt %d/%d), retrying in %.1fs: %s",
                            getattr(func, "__name__", "call"),
                            attempt,
                            attempts,
                            delay,
                            exc,
                        )
                    time.sleep(delay)
                    delay *= backoff
            raise RuntimeError("unreachable")  # pragma: no cover

        return wrapper  # type: ignore[return-value]

    return decorator


def require_non_empty_str(value: Any, name: str) -> str:
    """Validate that a value is a non-empty string and return it stripped.

    Args:
        value: Value to validate.
        name: Argument name used in the error message.

    Returns:
        The stripped string.

    Raises:
        ValueError: If ``value`` is not a string or is empty/whitespace.

    Example:
        >>> require_non_empty_str("  sales ", "table")
        'sales'
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"`{name}` must be a non-empty string, got: {value!r}")
    return value.strip()


def normalize_s3_path(path: Any, name: str = "path") -> str:
    """Validate and normalize an ``s3://`` URI.

    Args:
        path: Candidate S3 path.
        name: Argument name used in the error message.

    Returns:
        The stripped path, guaranteed to start with ``s3://``.

    Raises:
        ValueError: If the path is empty or does not start with ``s3://``.

    Example:
        >>> normalize_s3_path(" s3://bucket/data/ ")
        's3://bucket/data/'
    """
    cleaned = require_non_empty_str(path, name)
    if not cleaned.startswith("s3://"):
        raise ValueError(f"`{name}` must start with 's3://', got: {cleaned!r}")
    return cleaned
