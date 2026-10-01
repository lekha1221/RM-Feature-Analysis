"""Athena engine — runs SQL through AWS Athena via awswrangler.

Large-scale reads should go through Athena instead of direct S3 GETs: the
platform's S3 GET budget (~1,000 requests/minute) is far below request
volume, and Athena reads the data server-side.

``awswrangler`` is imported lazily so the rest of the library works without
it installed; install with ``pip install "s3dataloader[aws]"``.
"""

from __future__ import annotations

import os
from types import ModuleType
from typing import Any, Dict, Iterator, Optional, Sequence, Union

import pandas as pd

from dataSource.aws_auth import create_boto3_session
from dataSource.engines.base import BaseLoader, DateLike, FrameOrChunks
from dataSource.exceptions import AthenaError, ConfigurationError, LoaderError
from dataSource.hugeCollection_chunk_reading import (
    normalize_date_sql,
    require_full_date_filter,
)
from dataSource.utils import get_logger, log_timing, require_non_empty_str, with_retries

logger = get_logger("athena")

_RETRY_ATTEMPTS = 3
_RETRY_BASE_DELAY = 2.0
_THROTTLING_CODES = frozenset(
    {"ThrottlingException", "TooManyRequestsException", "SlowDown", "RequestLimitExceeded"}
)


def _import_awswrangler() -> ModuleType:
    """Import awswrangler lazily so the dependency stays optional."""
    try:
        import awswrangler  # noqa: PLC0415
    except ImportError as exc:
        raise ConfigurationError(
            "The 'athena' engine requires awswrangler. Install it with "
            "`pip install \"s3dataloader[aws]\"` or `pip install awswrangler`."
        ) from exc
    return awswrangler


def _is_throttling(exc: BaseException) -> bool:
    """Return True for boto3/awswrangler throttling errors worth retrying."""
    response = getattr(exc, "response", None) or {}
    return response.get("Error", {}).get("Code") in _THROTTLING_CODES


def _is_s3_result_forbidden(exc: BaseException) -> bool:
    """True when Athena succeeded but reading the result object from S3 is denied.

    Some accounts can ``StartQueryExecution`` while a bucket policy explicitly
    denies ``s3:GetObject`` / ``HeadObject`` / ``ListBucket`` on the results
    prefix. ``GetQueryResults`` still works in that case.
    """
    cur: BaseException | None = exc
    while cur is not None:
        response = getattr(cur, "response", None) or {}
        code = str(response.get("Error", {}).get("Code", ""))
        http = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        msg = str(cur)
        if http == 403 or code in {"403", "AccessDenied", "Forbidden", "AllAccessDisabled"}:
            if any(
                token in msg
                for token in (
                    "HeadObject",
                    "GetObject",
                    "ListBucket",
                    "ListObjects",
                    "Forbidden",
                    "Access Denied",
                    "AccessDenied",
                )
            ):
                return True
        cur = cur.__cause__ or cur.__context__
        if cur is exc:
            break
    return False


def _is_transient_endpoint(exc: BaseException) -> bool:
    """Retry DNS / connection drops during long GetQueryResults pagination."""
    name = type(exc).__name__
    msg = str(exc)
    tokens = (
        "EndpointConnectionError",
        "ConnectionClosedError",
        "ConnectTimeout",
        "ReadTimeout",
        "NameResolutionError",
        "nodename nor servname",
        "Could not connect to the endpoint URL",
        "gaierror",
    )
    if name in tokens or any(t in msg for t in tokens):
        return True
    cause = getattr(exc, "__cause__", None)
    return bool(cause) and _is_transient_endpoint(cause)


def _quote_ident(name: str, label: str = "identifier") -> str:
    """Double-quote a SQL identifier, rejecting embedded quotes."""
    cleaned = require_non_empty_str(name, label)
    if '"' in cleaned:
        raise ValueError(f"`{label}` must not contain double quotes: {cleaned!r}")
    return f'"{cleaned}"'


def _qualified_table(table: str) -> str:
    """Quote a possibly ``database.table``-qualified name part by part."""
    cleaned = require_non_empty_str(table, "table")
    return ".".join(_quote_ident(part, "table") for part in cleaned.split("."))


class AthenaLoader(BaseLoader):
    """Run SQL through AWS Athena and return pandas DataFrames.

    Purpose:
        Server-side SQL over the Glue catalog — the preferred engine for
        large scans, since it avoids per-file S3 GET requests from the
        client. Supports workgroups, custom result locations, the CTAS
        approach, result caching, and chunked loading.

    Supported operations:
        ``read_sql``, ``read_table``, ``scan``, ``load_dataframe``.
        ``read_parquet`` is not supported (use the wrangler or duckdb engine).

    Example:
        >>> loader = AthenaLoader(
        ...     database="leads-prod",
        ...     workgroup="asbl_analytics_prod",
        ...     output_location="s3://airflow-etl-db/athena_storage/",
        ... )
        >>> df = loader.read_sql("SELECT * FROM projects LIMIT 10")
    """

    engine = "athena"

    def __init__(
        self,
        database: Optional[str] = None,
        *,
        workgroup: Optional[str] = None,
        output_location: Optional[str] = None,
        region: Optional[str] = None,
        ctas_approach: bool = False,
        max_cache_seconds: int = 0,
        boto3_session: Optional[Any] = None,
    ) -> None:
        """Configure the Athena engine.

        Each setting falls back to its environment variable:
        ``ATHENA_DATABASE``, ``ATHENA_WORKGROUP``, ``ATHENA_OUTPUT_LOCATION``.

        Args:
            database: Glue database queries run against (required, or set
                ``ATHENA_DATABASE``).
            workgroup: Athena workgroup; omitted from calls when unset so the
                account default applies.
            output_location: ``s3://`` prefix for query results; omit when the
                workgroup enforces its own.
            region: AWS region override.
            ctas_approach: Use awswrangler's CTAS strategy (faster type-true
                results for large outputs, but needs Glue write permissions).
                Defaults to ``False``.
            max_cache_seconds: Reuse cached Athena results younger than this
                many seconds (0 disables caching).
            boto3_session: Pre-built boto3 session; by default one is created
                lazily preferring IAM roles/profiles over static keys.

        Raises:
            ConfigurationError: If no database is configured.
        """
        self._database = (database or os.getenv("ATHENA_DATABASE") or "").strip()
        if not self._database:
            raise ConfigurationError(
                "The athena engine needs a database: pass database= or set "
                "ATHENA_DATABASE in the environment."
            )
        self._workgroup = (workgroup or os.getenv("ATHENA_WORKGROUP") or "").strip()
        self._output_location = (
            output_location or os.getenv("ATHENA_OUTPUT_LOCATION") or ""
        ).strip()
        self._region = region
        self._ctas_approach = ctas_approach
        self._max_cache_seconds = max_cache_seconds
        self._boto3_session = boto3_session
        logger.info(
            "Athena engine configured (database=%r, workgroup=%r)",
            self._database,
            self._workgroup or None,
        )

    def read_sql(
        self,
        sql: str,
        *,
        database: Optional[str] = None,
        chunksize: Optional[int] = None,
        ctas_approach: Optional[bool] = None,
        max_cache_seconds: Optional[int] = None,
        **wr_kwargs: Any,
    ) -> FrameOrChunks:
        """Execute SQL on Athena via ``awswrangler.athena.read_sql_query``.

        Args:
            sql: Presto/Trino SQL text.
            database: Per-call database override.
            chunksize: When set, return an iterator of DataFrames of roughly
                this many rows instead of one DataFrame.
            ctas_approach: Per-call override of the constructor setting.
            max_cache_seconds: Per-call override of the cache window.
            **wr_kwargs: Extra keyword arguments passed straight to
                ``awswrangler.athena.read_sql_query``.

        Returns:
            A ``pandas.DataFrame``, or an iterator of DataFrames when
            ``chunksize`` is set.

        Raises:
            ValueError: If ``sql`` is empty.
            AthenaError: If the query fails (throttling errors are retried
                with exponential backoff first).

        Example:
            >>> df = loader.read_sql("SELECT * FROM sales LIMIT 100")
        """
        sql = require_non_empty_str(sql, "sql")
        wr = _import_awswrangler()
        params: Dict[str, Any] = {
            "sql": sql,
            "database": (database or self._database),
            "ctas_approach": self._ctas_approach if ctas_approach is None else ctas_approach,
            "boto3_session": self._session(),
        }
        if self._workgroup:
            params["workgroup"] = self._workgroup
        if self._output_location:
            params["s3_output"] = self._output_location
        cache_seconds = (
            self._max_cache_seconds if max_cache_seconds is None else max_cache_seconds
        )
        if cache_seconds:
            params["max_cache_seconds"] = cache_seconds
        if chunksize is not None:
            params["chunksize"] = chunksize
        params.update(wr_kwargs)

        runner = with_retries(
            attempts=_RETRY_ATTEMPTS,
            base_delay=_RETRY_BASE_DELAY,
            exceptions=(Exception,),
            retry_if=_is_throttling,
            logger=logger,
        )(wr.athena.read_sql_query)

        with log_timing(logger, "athena.read_sql") as stats:
            try:
                result = runner(**params)
            except LoaderError:
                raise
            except Exception as exc:
                if _is_s3_result_forbidden(exc):
                    logger.warning(
                        "Athena result S3 read forbidden (%s); falling back to GetQueryResults API",
                        exc,
                    )
                    result = self._read_sql_via_query_results(
                        sql,
                        database=(database or self._database),
                        chunksize=chunksize,
                    )
                else:
                    raise AthenaError(f"Athena query failed: {exc}") from exc
            if isinstance(result, pd.DataFrame):
                stats["rows"] = len(result)
        return result

    def _read_sql_via_query_results(
        self,
        sql: str,
        *,
        database: str,
        chunksize: Optional[int] = None,
    ) -> FrameOrChunks:
        """Materialise results via ``GetQueryResults`` (no S3 GetObject required)."""
        import time

        client = self._session().client("athena")
        start_kwargs: Dict[str, Any] = {
            "QueryString": sql,
            "QueryExecutionContext": {"Database": database},
        }
        if self._workgroup:
            start_kwargs["WorkGroup"] = self._workgroup
        if self._output_location:
            start_kwargs["ResultConfiguration"] = {"OutputLocation": self._output_location}

        qid = client.start_query_execution(**start_kwargs)["QueryExecutionId"]
        while True:
            execution = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            state = execution["Status"]["State"]
            if state == "SUCCEEDED":
                break
            if state in {"FAILED", "CANCELLED"}:
                reason = execution["Status"].get("StateChangeReason", state)
                raise AthenaError(f"Athena query {state}: {reason}")
            time.sleep(0.5)

        columns: list[str] | None = None
        buffer: list[list[Any]] = []
        chunks: list[pd.DataFrame] = []
        next_token: str | None = None

        def _flush() -> Optional[pd.DataFrame]:
            nonlocal buffer
            if columns is None or not buffer:
                return None
            frame = pd.DataFrame(buffer, columns=columns)
            buffer = []
            return frame

        while True:
            page_kwargs: Dict[str, Any] = {
                "QueryExecutionId": qid,
                "MaxResults": 1000,
            }
            if next_token:
                page_kwargs["NextToken"] = next_token

            def _get_page(kwargs: Dict[str, Any] = page_kwargs) -> Dict[str, Any]:
                return client.get_query_results(**kwargs)

            page = with_retries(
                attempts=_RETRY_ATTEMPTS,
                base_delay=_RETRY_BASE_DELAY,
                exceptions=(Exception,),
                retry_if=lambda exc: _is_throttling(exc) or _is_transient_endpoint(exc),
                logger=logger,
            )(_get_page)()
            rows = page["ResultSet"]["Rows"]
            start_idx = 0
            if columns is None:
                if not rows:
                    return pd.DataFrame() if chunksize is None else iter(())
                columns = [cell.get("VarCharValue", "") for cell in rows[0]["Data"]]
                start_idx = 1
            for row in rows[start_idx:]:
                buffer.append([cell.get("VarCharValue") for cell in row["Data"]])
                if chunksize is not None and len(buffer) >= int(chunksize):
                    frame = _flush()
                    if frame is not None:
                        chunks.append(frame)
            next_token = page.get("NextToken")
            if not next_token:
                break

        if chunksize is not None:
            frame = _flush()
            if frame is not None:
                chunks.append(frame)
            return iter(chunks)

        frame = _flush()
        return frame if frame is not None else pd.DataFrame(columns=columns or [])

    def read_table(
        self,
        table: str,
        *,
        columns: Optional[Sequence[str]] = None,
        date_column: Optional[str] = None,
        start_date: Optional[DateLike] = None,
        end_date: Optional[DateLike] = None,
        limit: Optional[int] = None,
        database: Optional[str] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Load an Athena table with optional projection and date filter.

        Args:
            table: Table name, optionally ``database.table``-qualified
                (e.g. ``"leads_analytics.facebook_ads_raw"``).
            columns: Optional column projection.
            date_column: Column for the inclusive date filter (all-or-none).
            start_date: Inclusive range start.
            end_date: Inclusive range end.
            limit: Optional ``LIMIT`` clause.
            database: Per-call database override.
            **kwargs: Forwarded to :meth:`read_sql` (e.g. ``chunksize``).

        Returns:
            A ``pandas.DataFrame``.

        Raises:
            ValueError: On partial date-filter arguments, invalid dates, or
                identifiers containing double quotes.
            AthenaError: If the query fails.

        Example:
            >>> df = loader.read_table(
            ...     "leadadreports",
            ...     columns=["adid", "campaignname"],
            ...     date_column="date",
            ...     start_date="2026-01-01", end_date="2026-01-31",
            ... )
        """
        table_sql = _qualified_table(table)
        date_filter = require_full_date_filter(date_column, start_date, end_date)
        select = (
            "*" if not columns else ", ".join(_quote_ident(c, "column") for c in columns)
        )
        sql = f"SELECT {select} FROM {table_sql}"
        if date_filter:
            dc, sd, ed = date_filter
            start_iso = normalize_date_sql(sd, "start_date")
            end_iso = normalize_date_sql(ed, "end_date")
            if start_iso > end_iso:
                raise ValueError(
                    f"`start_date` ({start_iso}) must be <= `end_date` ({end_iso})."
                )
            sql += (
                f" WHERE {_quote_ident(dc, 'date_column')} >= TIMESTAMP '{start_iso}'"
                f" AND {_quote_ident(dc, 'date_column')} <= TIMESTAMP '{end_iso}'"
            )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        result = self.read_sql(sql, database=database, **kwargs)
        if not isinstance(result, pd.DataFrame):
            raise AthenaError(
                "read_table returned a chunked result; use scan() or pass "
                "chunksize to read_sql for chunked loading."
            )
        return result

    def read_parquet(
        self,
        path: Optional[Union[str, Sequence[str]]] = None,
        *,
        columns: Optional[Sequence[str]] = None,
        **kwargs: Any,
    ) -> FrameOrChunks:
        """Not supported — Athena queries tables, not raw parquet paths.

        Args:
            path: Unused.
            columns: Unused.
            **kwargs: Unused.

        Raises:
            AthenaError: Always; points at the supported alternatives.

        Example:
            >>> loader.read_parquet("s3://bucket/data/")  # raises AthenaError
        """
        raise AthenaError(
            "The athena engine does not read parquet paths directly. Use "
            "read_sql()/read_table() against a catalog table, or the "
            "'wrangler' engine for direct S3 parquet reads."
        )

    def scan(
        self,
        target: str,
        *,
        chunksize: int = 10_000,
        **kwargs: Any,
    ) -> Iterator[pd.DataFrame]:
        """Stream a SQL result in DataFrame chunks.

        Args:
            target: SQL text to execute.
            chunksize: Approximate rows per chunk (default 10,000).
            **kwargs: Forwarded to :meth:`read_sql`.

        Returns:
            An iterator of ``pandas.DataFrame`` chunks.

        Raises:
            ValueError: If the SQL is empty.
            AthenaError: If the query fails.

        Example:
            >>> for chunk in loader.scan("SELECT * FROM big_table"):
            ...     process(chunk)
        """
        result = self.read_sql(target, chunksize=chunksize, **kwargs)
        if isinstance(result, pd.DataFrame):
            return iter([result])
        return result

    def _session(self) -> Any:
        """Lazily create (and cache) the boto3 session."""
        if self._boto3_session is None:
            self._boto3_session = create_boto3_session(self._region)
        return self._boto3_session
