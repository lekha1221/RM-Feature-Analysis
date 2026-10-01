"""DuckDB engine — wraps the existing :class:`~dataSource.getData.S3ParquetLoader`.

This engine reuses the battle-tested implementation (prefix resolution,
indexed-collection chunk pruning, the all-or-none date-filter rule) and only
adapts it to the common :class:`~dataSource.engines.base.BaseLoader`
interface. ``S3ParquetLoader`` itself is unchanged, so existing code keeps
working exactly as before.
"""

from __future__ import annotations

from typing import Any, Iterator, Optional, Sequence, Union

import duckdb
import pandas as pd

from dataSource.engines.base import BaseLoader, DateLike, FrameOrChunks
from dataSource.exceptions import ConfigurationError, DuckDBError
from dataSource.getData import S3ParquetLoader
from dataSource.hugeCollection_chunk_reading import (
    require_full_date_filter,
    wrap_sql_with_date_filter,
)
from dataSource.utils import get_logger, log_timing, require_non_empty_str

logger = get_logger("duckdb")

_FULL_SCAN_SQL = "SELECT * FROM parquet_scan('{parquet_path}')"


class DuckDBLoader(BaseLoader):
    """Load S3 parquet dumps with DuckDB (the original engine, default).

    Purpose:
        Direct S3 reads via DuckDB's ``parquet_scan``, including manifest
        chunk pruning for indexed collections. Best for ad-hoc reads that
        stay under the S3 GET request budget.

    Supported operations:
        ``read_parquet``, ``read_sql``, ``read_table``, ``scan``,
        ``load_dataframe``.

    Example:
        >>> loader = DuckDBLoader(source="leads-db")
        >>> df = loader.read_table(
        ...     "leads",
        ...     columns=["_id", "createdAt"],
        ...     date_column="createdAt",
        ...     start_date="2025-06-01",
        ...     end_date="2025-06-07",
        ... )
    """

    engine = "duckdb"

    def __init__(
        self,
        source: Optional[str] = "leads-db",
        *,
        s3_prefix: Optional[str] = None,
        bucket: Optional[str] = None,
    ) -> None:
        """Configure the DuckDB engine.

        Args:
            source: Named source ("leads-db", "crm") resolved through env keys.
            s3_prefix: Explicit dump prefix; overrides ``source``.
            bucket: S3 bucket; defaults to the ``S3_BUCKET`` env variable.

        Raises:
            ConfigurationError: If no S3 prefix can be resolved.
        """
        try:
            self._loader = S3ParquetLoader(source=source, s3_prefix=s3_prefix, bucket=bucket)
        except ValueError as exc:
            raise ConfigurationError(str(exc)) from exc
        logger.info("DuckDB engine configured (source=%r, s3_prefix=%r)", source, s3_prefix)

    @property
    def parquet_loader(self) -> S3ParquetLoader:
        """The wrapped :class:`S3ParquetLoader` (for backward-compatible access)."""
        return self._loader

    def read_parquet(
        self,
        path: Optional[Union[str, Sequence[str]]] = None,
        *,
        columns: Optional[Sequence[str]] = None,
        **kwargs: Any,
    ) -> FrameOrChunks:
        """Read parquet files directly with ``parquet_scan``.

        Args:
            path: A parquet path or glob — ``s3://bucket/prefix/*.parquet``
                or a local path. Lists are not supported by this engine.
            columns: Optional column projection.
            **kwargs: Unused; accepted for interface compatibility.

        Returns:
            A ``pandas.DataFrame``.

        Raises:
            ValueError: If ``path`` is missing or not a string.
            DuckDBError: If the query fails.

        Example:
            >>> df = loader.read_parquet(
            ...     "s3://airflow-etl-db/production_raw_dump/leads-db/leads/*.parquet",
            ...     columns=["_id"],
            ... )
        """
        if not isinstance(path, str):
            raise ValueError("DuckDBLoader.read_parquet requires `path` as a single string.")
        path = require_non_empty_str(path, "path")
        select = "*" if not columns else ", ".join(f'"{c}"' for c in columns)
        escaped = path.replace("'", "''")
        sql = f"SELECT {select} FROM parquet_scan('{escaped}')"
        with log_timing(logger, "duckdb.read_parquet") as stats:
            df = self._execute(sql)
            stats["rows"] = len(df)
        return df

    def read_sql(
        self,
        sql: str,
        *,
        collection: Optional[str] = None,
        date_column: Optional[str] = None,
        start_date: Optional[DateLike] = None,
        end_date: Optional[DateLike] = None,
        **kwargs: Any,
    ) -> FrameOrChunks:
        """Execute SQL against parquet data.

        With ``collection=``, this is the existing ``custom_loader`` behaviour:
        the ``{parquet_path}`` placeholder is substituted with the collection's
        glob or manifest-pruned chunk list. Without it, the SQL runs verbatim
        on the shared DuckDB connection.

        Args:
            sql: SQL text, optionally containing ``{parquet_path}``.
            collection: Collection the placeholder resolves to.
            date_column: Column for the inclusive date filter (all-or-none).
            start_date: Inclusive range start.
            end_date: Inclusive range end.
            **kwargs: Unused; accepted for interface compatibility.

        Returns:
            A ``pandas.DataFrame``.

        Raises:
            ConfigurationError: If ``sql`` uses ``{parquet_path}`` without
                ``collection=``.
            ValueError: On empty SQL or partial date-filter arguments.
            DuckDBError: If the query fails.

        Example:
            >>> df = loader.read_sql(
            ...     "SELECT leadId, COUNT(*) n FROM parquet_scan('{parquet_path}') GROUP BY 1",
            ...     collection="whatsappchatmessages",
            ...     date_column="createdAt", start_date="2025-06-01", end_date="2025-06-07",
            ... )
        """
        sql = require_non_empty_str(sql, "sql")
        if collection is not None:
            with log_timing(logger, "duckdb.read_sql") as stats:
                df = self._wrap_errors(
                    lambda: self._loader.custom_loader(
                        sql,
                        collection,
                        date_column=date_column,
                        start_date=start_date,
                        end_date=end_date,
                    )
                )
                stats["rows"] = len(df)
            return df

        if "{parquet_path}" in sql:
            raise ConfigurationError(
                "SQL contains the '{parquet_path}' placeholder but no collection= "
                "was given; pass collection='<name>' so the path can be resolved."
            )
        date_filter = require_full_date_filter(date_column, start_date, end_date)
        if date_filter:
            sql = wrap_sql_with_date_filter(sql, *date_filter)
        with log_timing(logger, "duckdb.read_sql") as stats:
            df = self._execute(sql)
            stats["rows"] = len(df)
        return df

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
        """Load a collection, optionally projecting columns and filtering dates.

        Delegates to ``S3ParquetLoader.loader`` when columns are requested and
        to ``custom_loader`` (``SELECT *``) for full-width loads, so indexed
        collections keep their manifest chunk pruning in both cases.

        Args:
            table: Collection name (e.g. ``"leads"``, ``"visitorlogs"``).
            columns: Optional column projection; ``None`` loads all columns.
            date_column: Column for the inclusive date filter (all-or-none).
            start_date: Inclusive range start.
            end_date: Inclusive range end.
            **kwargs: Unused; accepted for interface compatibility.

        Returns:
            A ``pandas.DataFrame``.

        Raises:
            ValueError: On partial date-filter arguments, unknown
                ``date_column``, or when none of the requested columns exist.
            DuckDBError: If the query fails.

        Example:
            >>> df = loader.read_table("leads", columns=["_id", "phone"])
        """
        table = require_non_empty_str(table, "table")
        with log_timing(logger, "duckdb.read_table") as stats:
            if columns is not None:
                df = self._wrap_errors(
                    lambda: self._loader.loader(
                        table, list(columns), date_column, start_date, end_date
                    )
                )
            else:
                df = self._wrap_errors(
                    lambda: self._loader.custom_loader(
                        _FULL_SCAN_SQL,
                        table,
                        date_column=date_column,
                        start_date=start_date,
                        end_date=end_date,
                    )
                )
            stats["rows"] = len(df)
        return df

    def scan(
        self,
        target: str,
        *,
        collection: Optional[str] = None,
        **kwargs: Any,
    ) -> Iterator[pd.DataFrame]:
        """Stream a SQL result in DataFrame chunks.

        Note:
            Unlike ``read_sql``, ``scan`` substitutes ``{parquet_path}`` with
            the collection's full glob without manifest date pruning — apply
            filters in the SQL itself.

        Args:
            target: SQL text, optionally containing ``{parquet_path}``.
            collection: Collection the placeholder resolves to.
            **kwargs: Unused; accepted for interface compatibility.

        Returns:
            An iterator of ``pandas.DataFrame`` chunks.

        Raises:
            ConfigurationError: If ``{parquet_path}`` is used without
                ``collection=``.
            DuckDBError: If the query fails during iteration.

        Example:
            >>> for chunk in loader.scan(
            ...     "SELECT * FROM parquet_scan('{parquet_path}')", collection="leads"
            ... ):
            ...     process(chunk)
        """
        sql = require_non_empty_str(target, "target")
        if collection is not None:
            bucket, prefix, _ = self._loader._resolve_location()
            parquet_path = self._loader._build_parquet_path(collection, prefix, bucket)
            sql = sql.replace("{parquet_path}", parquet_path)
        elif "{parquet_path}" in sql:
            raise ConfigurationError(
                "SQL contains the '{parquet_path}' placeholder but no collection= "
                "was given; pass collection='<name>' so the path can be resolved."
            )
        con = self._connection()

        def _chunks() -> Iterator[pd.DataFrame]:
            total = 0
            with log_timing(logger, "duckdb.scan") as stats:
                try:
                    result = con.execute(sql)
                    while True:
                        chunk = result.fetch_df_chunk()
                        if chunk is None or len(chunk) == 0:
                            break
                        total += len(chunk)
                        yield chunk
                except duckdb.Error as exc:
                    raise DuckDBError(f"DuckDB scan failed: {exc}") from exc
                stats["rows"] = total

        return _chunks()

    def _connection(self) -> duckdb.DuckDBPyConnection:
        return self._loader._get_connection()

    def _execute(self, sql: str) -> pd.DataFrame:
        return self._wrap_errors(lambda: self._connection().execute(sql).fetchdf())

    @staticmethod
    def _wrap_errors(call: Any) -> pd.DataFrame:
        try:
            return call()
        except duckdb.Error as exc:
            raise DuckDBError(f"DuckDB query failed: {exc}") from exc
