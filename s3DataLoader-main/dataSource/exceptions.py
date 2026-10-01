"""Custom exception hierarchy for the dataSource loader library.

All engine failures are surfaced as subclasses of :class:`LoaderError`, so
callers can catch one base class regardless of the configured backend.

Example:
    >>> from dataSource.exceptions import LoaderError
    >>> try:
    ...     df = loader.read_sql("SELECT 1")
    ... except LoaderError as exc:
    ...     print(f"load failed: {exc}")
"""

from __future__ import annotations


class LoaderError(Exception):
    """Base class for every error raised by the dataSource loaders."""


class ConfigurationError(LoaderError):
    """Raised for invalid configuration: unknown engine, missing settings,
    or call arguments that cannot be resolved (e.g. no database configured).
    """


class DuckDBError(LoaderError):
    """Raised when the DuckDB engine fails to execute a query."""


class AthenaError(LoaderError):
    """Raised when an Athena query fails or an unsupported Athena operation
    is requested.
    """


class WranglerError(LoaderError):
    """Raised when an AWS Wrangler S3 read fails or an unsupported wrangler
    operation is requested.
    """
