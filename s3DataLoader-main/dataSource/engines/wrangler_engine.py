"""AWS Wrangler engine — direct S3 parquet reads via ``awswrangler.s3.read_parquet``.

Supports dataset mode, partition filtering, column projection, threading,
chunked reading, path lists, and S3 prefixes. Collection names resolve to
the same ``{prefix}/{collection}`` layout the DuckDB engine uses (indexed
collections read from their ``data/`` subdirectory).

``awswrangler`` is imported lazily so the rest of the library works without
it installed; install with ``pip install "s3dataloader[aws]"``.
"""

from __future__ import annotations

import os
from types import ModuleType
from typing import Any, Dict, Iterator, List, Optional, Sequence, Union

import pandas as pd

from dataSource.aws_auth import create_boto3_session
from dataSource.engines.base import BaseLoader, DateLike, FrameOrChunks
from dataSource.exceptions import ConfigurationError, LoaderError, WranglerError
from dataSource.hugeCollection_chunk_reading import (
    INDEXED_COLLECTIONS,
    normalize_date_sql,
    require_full_date_filter,
)
from dataSource.s3_config import resolve_s3_prefix
from dataSource.utils import (
    get_logger,
    log_timing,
    normalize_s3_path,
    require_non_empty_str,
)

logger = get_logger("wrangler")


def _import_awswrangler() -> ModuleType:
    """Import awswrangler lazily so the dependency stays optional."""
    try:
        import awswrangler  # noqa: PLC0415
    except ImportError as exc:
        raise ConfigurationError(
            "The 'wrangler' engine requires awswrangler. Install it with "
            "`pip install \"s3dataloader[aws]\"` or `pip install awswrangler`."
        ) from exc
    return awswrangler


class WranglerLoader(BaseLoader):
    """Read parquet data straight from S3 with AWS Wrangler.

    Purpose:
        Bulk parquet reads (dataset mode, partition filters, threading,
        chunking) without a SQL engine. Good when you want whole datasets or
        partition-pruned slices as DataFrames.

    Supported operations:
        ``read_parquet``, ``read_table``, ``scan``, ``load_dataframe``.
        ``read_sql`` is not supported (use the athena or duckdb engine).

    Example:
        >>> loader = WranglerLoader(source="leads-db")
        >>> df = loader.read_parquet(
        ...     path="s3://bucket/data/",
        ...     columns=["id", "price"],
        ... )
    """

    engine = "wrangler"

    def __init__(
        self,
        source: Optional[str] = "leads-db",
        *,
        s3_prefix: Optional[str] = None,
        bucket: Optional[str] = None,
        region: Optional[str] = None,
        use_threads: Union[bool, int] = True,
        boto3_session: Optional[Any] = None,
    ) -> None:
        """Configure the wrangler engine.

        Args:
            source: Named source ("leads-db", "crm") used when reading
                collections by name; resolved through the same env keys as
                the DuckDB engine.
            s3_prefix: Explicit dump prefix; overrides ``source``.
            bucket: S3 bucket; defaults to the ``S3_BUCKET`` env variable.
            region: AWS region override.
            use_threads: ``True`` (all cores), ``False``, or a thread count —
                passed to awswrangler.
            boto3_session: Pre-built boto3 session; by default one is created
                lazily preferring IAM roles/profiles over static keys.
        """
        self._source = source
        self._prefix_override = s3_prefix
        self._bucket = (bucket or os.getenv("S3_BUCKET") or "").strip()
        self._region = region
        self._use_threads = use_threads
        self._boto3_session = boto3_session
        logger.info(
            "Wrangler engine configured (source=%r, s3_prefix=%r)", source, s3_prefix
        )

    def read_parquet(
        self,
        path: Optional[Union[str, Sequence[str]]] = None,
        *,
        collection: Optional[str] = None,
        columns: Optional[Sequence[str]] = None,
        partition_filter: Optional[Any] = None,
        dataset: bool = True,
        chunked: Union[bool, int] = False,
        path_suffix: Optional[str] = ".parquet",
        **wr_kwargs: Any,
    ) -> FrameOrChunks:
        """Read parquet data from S3 via ``awswrangler.s3.read_parquet``.

        Exactly one of ``path`` or ``collection`` must be given.

        Args:
            path: ``s3://`` prefix, single object path, or list of paths.
            collection: Collection name resolved to
                ``s3://{bucket}/{prefix}/{collection}/`` (indexed collections
                read from their ``data/`` subdirectory).
            columns: Optional column projection.
            partition_filter: awswrangler partition filter callable, e.g.
                ``lambda p: p["year"] == "2026"`` (partitioned datasets only).
            dataset: Read as a partition-aware dataset (default ``True``).
            chunked: ``True`` for memory-friendly chunks, or an int for a
                fixed number of rows per chunk; returns an iterator.
            path_suffix: Only read objects with this suffix (default
                ``".parquet"``; pass ``None`` to disable).
            **wr_kwargs: Extra keyword arguments passed straight to
                ``awswrangler.s3.read_parquet``.

        Returns:
            A ``pandas.DataFrame``, or an iterator of DataFrames when
            ``chunked`` is set.

        Raises:
            ValueError: If neither or both of ``path``/``collection`` are
                given, or a path is not an ``s3://`` URI.
            ConfigurationError: If a collection is requested but no bucket is
                configured.
            WranglerError: If the S3 read fails.

        Example:
            >>> df = loader.read_parquet(path="s3://bucket/data/", columns=["id"])
        """
        resolved = self._resolve_target(path, collection)
        wr = _import_awswrangler()
        params: Dict[str, Any] = {
            "path": resolved,
            "dataset": dataset,
            "use_threads": self._use_threads,
            "boto3_session": self._session(),
        }
        if columns is not None:
            params["columns"] = list(columns)
        if partition_filter is not None:
            params["partition_filter"] = partition_filter
        if chunked:
            params["chunked"] = chunked
        if path_suffix is not None:
            params["path_suffix"] = path_suffix
        params.update(wr_kwargs)

        with log_timing(logger, "wrangler.read_parquet") as stats:
            try:
                result = wr.s3.read_parquet(**params)
            except LoaderError:
                raise
            except Exception as exc:
                raise WranglerError(f"S3 parquet read failed for {resolved!r}: {exc}") from exc
            if isinstance(result, pd.DataFrame):
                stats["rows"] = len(result)
        return result

    def read_sql(self, sql: str, **kwargs: Any) -> FrameOrChunks:
        """Not supported — AWS Wrangler's S3 reader has no SQL engine.

        Args:
            sql: Unused.
            **kwargs: Unused.

        Raises:
            WranglerError: Always; points at the supported alternatives.

        Example:
            >>> loader.read_sql("SELECT 1")  # raises WranglerError
        """
        raise WranglerError(
            "The wrangler engine cannot execute SQL. Use engine='athena' for "
            "SQL over the catalog, engine='duckdb' for SQL over parquet, or "
            "read_parquet() for direct S3 reads."
        )

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
        """Load a collection with optional projection and date filter.

        Note:
            Date filtering is applied client-side (in pandas) after the read,
            because plain S3 reads cannot push down row predicates. For large
            date-filtered loads prefer the athena or duckdb engine.

        Args:
            table: Collection name (e.g. ``"leads"``).
            columns: Optional column projection.
            date_column: Column for the inclusive date filter (all-or-none).
            start_date: Inclusive range start.
            end_date: Inclusive range end.
            **kwargs: Forwarded to :meth:`read_parquet`.

        Returns:
            A ``pandas.DataFrame``.

        Raises:
            ValueError: On partial date-filter arguments, invalid dates, or
                when ``date_column`` is missing from the data.
            WranglerError: If the S3 read fails.

        Example:
            >>> df = loader.read_table("leads", columns=["_id", "createdAt"])
        """
        table = require_non_empty_str(table, "table")
        date_filter = require_full_date_filter(date_column, start_date, end_date)

        fetch_columns: Optional[List[str]] = list(columns) if columns is not None else None
        if date_filter and fetch_columns is not None and date_filter[0] not in fetch_columns:
            fetch_columns = fetch_columns + [date_filter[0]]

        result = self.read_parquet(collection=table, columns=fetch_columns, **kwargs)
        if not isinstance(result, pd.DataFrame):
            raise WranglerError(
                "read_table returned a chunked result; use scan() for chunked reading."
            )
        df = result

        if date_filter:
            dc, sd, ed = date_filter
            if dc not in df.columns:
                raise ValueError(
                    f"`date_column` {dc!r} is not present in collection {table!r}. "
                    f"Available columns: {list(df.columns)}"
                )
            start_ts = pd.Timestamp(normalize_date_sql(sd, "start_date"))
            end_ts = pd.Timestamp(normalize_date_sql(ed, "end_date"))
            if start_ts > end_ts:
                raise ValueError("`start_date` must be <= `end_date`.")
            logger.warning(
                "wrangler.read_table filters %r client-side after the S3 read; "
                "prefer the athena engine for large date-filtered loads.",
                dc,
            )
            col = pd.to_datetime(df[dc], errors="coerce")
            if getattr(col.dt, "tz", None) is not None:
                col = col.dt.tz_localize(None)
            df = df.loc[(col >= start_ts) & (col <= end_ts)]
            if columns is not None:
                df = df[list(columns)]
        return df.reset_index(drop=True) if date_filter else df

    def scan(
        self,
        target: str,
        *,
        columns: Optional[Sequence[str]] = None,
        chunked: Union[bool, int] = True,
        **kwargs: Any,
    ) -> Iterator[pd.DataFrame]:
        """Stream parquet data in DataFrame chunks.

        Args:
            target: ``s3://`` prefix, or a collection name.
            columns: Optional column projection.
            chunked: ``True`` (memory-friendly chunks) or rows per chunk.
            **kwargs: Forwarded to :meth:`read_parquet`.

        Returns:
            An iterator of ``pandas.DataFrame`` chunks.

        Raises:
            ValueError: If the target is invalid.
            WranglerError: If the S3 read fails.

        Example:
            >>> for chunk in loader.scan("visitorlogs", chunked=100_000):
            ...     process(chunk)
        """
        target = require_non_empty_str(target, "target")
        chunked = chunked or True
        if target.startswith("s3://"):
            result = self.read_parquet(path=target, columns=columns, chunked=chunked, **kwargs)
        else:
            result = self.read_parquet(
                collection=target, columns=columns, chunked=chunked, **kwargs
            )
        if isinstance(result, pd.DataFrame):  # pragma: no cover - defensive
            return iter([result])
        return result

    def _resolve_target(
        self,
        path: Optional[Union[str, Sequence[str]]],
        collection: Optional[str],
    ) -> Union[str, List[str]]:
        """Validate and resolve the read target to an S3 path or path list."""
        if (path is None) == (collection is None):
            raise ValueError(
                "Pass exactly one of `path` (s3:// prefix or list of paths) "
                "or `collection` (collection name)."
            )
        if path is not None:
            if isinstance(path, str):
                return normalize_s3_path(path)
            return [normalize_s3_path(p, "path") for p in path]

        collection = require_non_empty_str(collection, "collection")
        if not self._bucket:
            raise ConfigurationError(
                "S3_BUCKET must be set (env or bucket=) to read collections by name."
            )
        prefix = resolve_s3_prefix(source=self._source, s3_prefix=self._prefix_override)
        base = f"s3://{self._bucket}/{prefix}/{collection}"
        if collection in INDEXED_COLLECTIONS:
            return f"{base}/data/"
        return f"{base}/"

    def _session(self) -> Any:
        """Lazily create (and cache) the boto3 session."""
        if self._boto3_session is None:
            self._boto3_session = create_boto3_session(self._region)
        return self._boto3_session
