"""Abstract base class defining the common loader interface.

Every engine (DuckDB, Athena, Wrangler, and future backends) implements the
same five methods, so application code can switch engines without changing
its call sites:

- ``read_parquet`` — read parquet data directly from S3.
- ``read_sql`` — execute SQL and return the result.
- ``read_table`` — load a table/collection with optional column projection
  and an inclusive date-range filter.
- ``scan`` — lazily iterate over a result in DataFrame chunks.
- ``load_dataframe`` — eager convenience alias for ``read_table``.

Not every operation is native to every backend; an engine that cannot
support one raises its engine error with an actionable message (e.g. the
wrangler engine cannot execute SQL and points you at the athena engine).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, ClassVar, Iterator, Optional, Sequence, Union

import pandas as pd

DateLike = Union[str, pd.Timestamp, datetime]
FrameOrChunks = Union[pd.DataFrame, Iterator[pd.DataFrame]]


class BaseLoader(ABC):
    """Abstract interface shared by all data-loading engines.

    Purpose:
        Defines the engine-agnostic API. Concrete engines encapsulate all
        backend-specific logic (connections, sessions, SQL dialects) behind
        these methods.

    Supported operations:
        ``read_parquet``, ``read_sql``, ``read_table``, ``scan``,
        ``load_dataframe``.

    Example:
        >>> from dataSource import DataLoader
        >>> loader = DataLoader(engine="duckdb")          # or "athena"/"wrangler"
        >>> df = loader.read_table("leads", columns=["_id", "createdAt"])
    """

    #: Canonical engine name used by :class:`~dataSource.factory.LoaderFactory`.
    engine: ClassVar[str] = "base"

    @abstractmethod
    def read_parquet(
        self,
        path: Optional[Union[str, Sequence[str]]] = None,
        *,
        columns: Optional[Sequence[str]] = None,
        **kwargs: Any,
    ) -> FrameOrChunks:
        """Read parquet data from S3.

        Args:
            path: ``s3://`` prefix, file path, or list of paths.
            columns: Optional column projection.
            **kwargs: Engine-specific options (see the concrete engine docs).

        Returns:
            A ``pandas.DataFrame``, or an iterator of DataFrames when the
            engine is asked for chunked reading.

        Raises:
            LoaderError: On engine failure, or if the engine does not support
                direct parquet reads (e.g. Athena).
            ValueError: If the arguments are invalid.
        """

    @abstractmethod
    def read_sql(self, sql: str, **kwargs: Any) -> FrameOrChunks:
        """Execute a SQL query and return the result.

        Args:
            sql: The SQL text. DuckDB supports a ``{parquet_path}``
                placeholder when ``collection=`` is passed; Athena executes
                Presto/Trino SQL against the Glue catalog.
            **kwargs: Engine-specific options (see the concrete engine docs).

        Returns:
            A ``pandas.DataFrame``, or an iterator of DataFrames when a
            ``chunksize`` is requested.

        Raises:
            LoaderError: On engine failure, or if the engine cannot execute
                SQL (e.g. the wrangler engine).
            ValueError: If ``sql`` is empty or arguments are invalid.
        """

    @abstractmethod
    def read_table(
        self,
        table: str,
        *,
        columns: Optional[Sequence[str]] = None,
        date_column: Optional[str] = None,
        start_date: Optional[DateLike] = None,
        end_date: Optional[DateLike] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Load a table/collection with optional projection and date filter.

        ``date_column``, ``start_date``, and ``end_date`` are all-or-none:
        pass all three for an inclusive date-range filter, or none for a
        full load. Passing one or two raises ``ValueError``.

        Args:
            table: Collection name (DuckDB/wrangler) or table name, optionally
                ``database.table``-qualified (Athena).
            columns: Optional column projection.
            date_column: Column to filter on.
            start_date: Inclusive range start (str, datetime, or Timestamp).
            end_date: Inclusive range end.
            **kwargs: Engine-specific options.

        Returns:
            A ``pandas.DataFrame``.

        Raises:
            LoaderError: On engine failure.
            ValueError: On partial date-filter arguments or invalid dates.
        """

    @abstractmethod
    def scan(self, target: str, **kwargs: Any) -> Iterator[pd.DataFrame]:
        """Lazily iterate over a result in DataFrame chunks.

        Args:
            target: Engine-specific target — SQL text for DuckDB/Athena, an
                ``s3://`` path or collection name for the wrangler engine.
            **kwargs: Engine-specific options (e.g. ``chunksize``).

        Returns:
            An iterator of ``pandas.DataFrame`` chunks.

        Raises:
            LoaderError: On engine failure.
            ValueError: If the target is invalid.
        """

    def load_dataframe(
        self,
        table: str,
        *,
        columns: Optional[Sequence[str]] = None,
        date_column: Optional[str] = None,
        start_date: Optional[DateLike] = None,
        end_date: Optional[DateLike] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Eagerly load a table into a DataFrame (alias for ``read_table``).

        Args:
            table: Table or collection name.
            columns: Optional column projection.
            date_column: Column to filter on (all-or-none with the dates).
            start_date: Inclusive range start.
            end_date: Inclusive range end.
            **kwargs: Engine-specific options.

        Returns:
            A ``pandas.DataFrame``.

        Raises:
            LoaderError: On engine failure.
            ValueError: On partial date-filter arguments.

        Example:
            >>> df = loader.load_dataframe("leads", columns=["_id"])
        """
        return self.read_table(
            table,
            columns=columns,
            date_column=date_column,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )
