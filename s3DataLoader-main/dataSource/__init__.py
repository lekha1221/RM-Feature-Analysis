"""S3 parquet loading for call and WhatsApp cohort pipelines."""

import logging as _logging

from dataSource.getData import S3ParquetLoader
from dataSource.hugeCollection_chunk_reading import (
    INDEXED_COLLECTIONS,
    S3IndexedCollectionReader,
    normalize_date_sql,
    require_full_date_filter,
    wrap_sql_with_date_filter,
)
from dataSource.s3_config import resolve_s3_prefix
from dataSource.engines import AthenaLoader, BaseLoader, DuckDBLoader, WranglerLoader
from dataSource.exceptions import (
    AthenaError,
    ConfigurationError,
    DuckDBError,
    LoaderError,
    WranglerError,
)
from dataSource.factory import DataLoader, LoaderFactory

_logging.getLogger("dataSource").addHandler(_logging.NullHandler())

__all__ = [
    # Original API (unchanged)
    "S3ParquetLoader",
    "S3IndexedCollectionReader",
    "INDEXED_COLLECTIONS",
    "resolve_s3_prefix",
    "normalize_date_sql",
    "require_full_date_filter",
    "wrap_sql_with_date_filter",
    # Engine-based API
    "DataLoader",
    "LoaderFactory",
    "BaseLoader",
    "DuckDBLoader",
    "AthenaLoader",
    "WranglerLoader",
    # Exceptions
    "LoaderError",
    "ConfigurationError",
    "DuckDBError",
    "AthenaError",
    "WranglerError",
]
