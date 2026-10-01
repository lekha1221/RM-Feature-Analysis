"""Fetch parquet data from S3 using DuckDB."""

from __future__ import annotations

import os
from typing import Iterable, Optional, Union

import duckdb
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from dataSource.hugeCollection_chunk_reading import (
    INDEXED_COLLECTIONS,
    S3IndexedCollectionReader,
    normalize_date_sql,
    require_full_date_filter,
    wrap_sql_with_date_filter,
)
from dataSource.aws_auth import configure_duckdb_s3
from dataSource.s3_config import normalize_source, resolve_s3_prefix

load_dotenv()

BUCKET_NAME = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")

DateLike = Union[str, pd.Timestamp, "datetime.datetime"]

_CONNECTION: Optional[duckdb.DuckDBPyConnection] = None


class S3ParquetLoader:

    def __init__(
        self,
        source: str | None = "leads-db",
        s3_prefix: str | None = None,
        bucket: str | None = None,
    ):
        self._default_source = source
        self._default_prefix_override = s3_prefix
        self._bucket = (bucket or BUCKET_NAME or os.getenv("S3_BUCKET") or "").strip()
        self._prefix = resolve_s3_prefix(source=source, s3_prefix=s3_prefix)

    def _resolve_location(
        self,
        source: str | None = None,
        s3_prefix: str | None = None,
    ) -> tuple[str, str, str]:
        """Return (bucket, prefix, canonical_source_label) for this call."""
        if s3_prefix is not None and str(s3_prefix).strip():
            prefix = str(s3_prefix).strip().rstrip("/")
            label = (source or self._default_source or "custom").strip()
        elif source is not None:
            prefix = resolve_s3_prefix(source=source, s3_prefix=None)
            label = normalize_source(str(source))
        else:
            prefix = (
                resolve_s3_prefix(
                    source=self._default_source,
                    s3_prefix=self._default_prefix_override,
                )
                if self._default_prefix_override
                else self._prefix
            )
            label = normalize_source(str(self._default_source or "leads-db"))

        bucket = self._bucket
        if not bucket:
            raise EnvironmentError("S3_BUCKET must be set in the environment.")
        if not prefix:
            raise EnvironmentError(
                "S3 prefix could not be resolved. Set S3_PREFIX_LEADS_DB / "
                "S3_PREFIX_CRM / S3_PREFIX, or pass s3_prefix=."
            )
        return bucket, prefix, label

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Return a process-wide DuckDB connection with S3 credentials configured."""
        global _CONNECTION
        if _CONNECTION is None:
            con = duckdb.connect()
            configure_duckdb_s3(con)
            _CONNECTION = con
        return _CONNECTION

    def _build_parquet_path(self, collection_name: str, prefix: str, bucket: str) -> str:
        if not isinstance(collection_name, str) or not collection_name.strip():
            raise ValueError(
                f"`collection_name` must be a non-empty string, got: {collection_name!r}"
            )
        base = f"s3://{bucket}/{prefix}/{collection_name.strip()}"
        if collection_name.strip() in INDEXED_COLLECTIONS:
            return f"{base}/data/*.parquet"
        return f"{base}/*.parquet"

    def _normalize_date(self, value: DateLike, label: str) -> str:
        return normalize_date_sql(value, label)

    def _get_metadata_columns(
        self, con: duckdb.DuckDBPyConnection, parquet_path: str
    ) -> list[str]:
        return (
            con.execute(f"DESCRIBE SELECT * FROM parquet_scan('{parquet_path}')")
            .df()["column_name"]
            .tolist()
        )

    def _resolve_columns(
        self,
        collection_name: str,
        column_list: Iterable[str],
        metadata_columns: list[str],
    ) -> list[str]:
        requested = list(dict.fromkeys(column_list or []))
        if not requested:
            raise ValueError("`column_list` cannot be empty.")

        missing = [c for c in requested if c not in metadata_columns]
        present = [c for c in requested if c in metadata_columns]

        if missing:
            print(
                f"[loader] columns not found in '{collection_name}' metadata "
                f"(skipped): {missing}"
            )

        if not present:
            raise ValueError(
                f"None of the requested columns exist in collection {collection_name!r}."
            )
        return present

    def _indexed_reader(
        self, collection: str, source: str | None, s3_prefix: str | None
    ) -> S3IndexedCollectionReader:
        bucket, prefix, label = self._resolve_location(source=source, s3_prefix=s3_prefix)
        return S3IndexedCollectionReader(
            bucket=bucket,
            database=label,
            collection=collection,
            s3_prefix=prefix,
        )

    def loader(
        self,
        collection_name: str,
        column_list: Iterable[str],
        date_column: str | None = None,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
        *,
        source: str | None = None,
        s3_prefix: str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch data from S3 parquet for a given collection, returning only the
        requested columns.

        Pass ``date_column`` + ``start_date`` + ``end_date`` together to filter
        rows inclusively; omit all three for a full collection load.

        Indexed collections use manifest chunk pruning; others scan the flat glob.

        Optional ``source`` / ``s3_prefix`` override the loader's constructor defaults
        for this call only.
        """
        collection = collection_name.strip()
        date_filter = require_full_date_filter(date_column, start_date, end_date)

        if collection in INDEXED_COLLECTIONS:
            reader = self._indexed_reader(collection, source, s3_prefix)
            metadata_columns = reader.describe_columns()
            if date_filter and date_filter[0] not in metadata_columns:
                raise ValueError(
                    f"`date_column` {date_filter[0]!r} is not present in collection "
                    f"{collection_name!r}. Available columns: {metadata_columns}"
                )
            present = self._resolve_columns(collection, column_list, metadata_columns)
            if date_filter:
                dc, sd, ed = date_filter
                return reader.read_range(
                    date_column=dc,
                    start_date=sd,
                    end_date=ed,
                    columns=present,
                )
            return reader.read_range(columns=present)

        bucket, prefix, _ = self._resolve_location(source=source, s3_prefix=s3_prefix)
        parquet_path = self._build_parquet_path(collection, prefix, bucket)
        con = self._get_connection()
        metadata_columns = self._get_metadata_columns(con, parquet_path)

        if date_filter and date_filter[0] not in metadata_columns:
            raise ValueError(
                f"`date_column` {date_filter[0]!r} is not present in collection "
                f"{collection_name!r}. Available columns: {metadata_columns}"
            )

        present = self._resolve_columns(collection, column_list, metadata_columns)
        select_cols = ", ".join(f'"{c}"' for c in present)
        if date_filter:
            dc, sd, ed = date_filter
            start_iso = self._normalize_date(sd, "start_date")
            end_iso = self._normalize_date(ed, "end_date")
            if start_iso > end_iso:
                raise ValueError(
                    f"`start_date` ({start_iso}) must be <= `end_date` ({end_iso})."
                )
            sql = f"""
                SELECT {select_cols}
                FROM parquet_scan('{parquet_path}')
                WHERE "{dc}" >= TIMESTAMP '{start_iso}'
                  AND "{dc}" <= TIMESTAMP '{end_iso}'
            """
        else:
            sql = f"""
                SELECT {select_cols}
                FROM parquet_scan('{parquet_path}')
            """
        return con.execute(sql).fetchdf()

    def custom_loader(
        self,
        sql_query: str,
        collection_name: str,
        *,
        date_column: str | None = None,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
        source: str | None = None,
        s3_prefix: str | None = None,
    ) -> pd.DataFrame:
        """
        Execute custom SQL against a collection's parquet data.

        Use ``{parquet_path}`` in SQL as the table source placeholder.

        When ``date_column`` + ``start_date`` + ``end_date`` are all provided:
        - indexed collections: manifest chunk pruning + row filter
        - other collections: row filter on your query results

        Optional ``source`` / ``s3_prefix`` override the loader's constructor defaults
        for this call only.
        """
        if not isinstance(sql_query, str) or not sql_query.strip():
            raise ValueError("`sql_query` must be a non-empty string.")

        collection = collection_name.strip()
        date_filter = require_full_date_filter(date_column, start_date, end_date)

        if collection in INDEXED_COLLECTIONS:
            reader = self._indexed_reader(collection, source, s3_prefix)
            return reader.read_sql(
                sql_query,
                date_column=date_filter[0] if date_filter else None,
                start_date=date_filter[1] if date_filter else None,
                end_date=date_filter[2] if date_filter else None,
            )

        bucket, prefix, _ = self._resolve_location(source=source, s3_prefix=s3_prefix)
        parquet_path = self._build_parquet_path(collection, prefix, bucket)
        con = self._get_connection()

        if "{parquet_path}" in sql_query:
            sql_query = sql_query.replace("{parquet_path}", parquet_path)

        if date_filter:
            dc, sd, ed = date_filter
            sql_query = wrap_sql_with_date_filter(sql_query, dc, sd, ed)

        return con.execute(sql_query).fetchdf()


if __name__ == "__main__":
    # ponytail: resolve helper smoke — fails if env mapping regresses
    saved = {
        k: os.environ.get(k)
        for k in (
            "S3_PREFIX",
            "S3_PREFIX_LEADS_DB",
            "S3_PREFIX_CRM",
        )
    }
    try:
        for k in saved:
            os.environ.pop(k, None)

        os.environ["S3_PREFIX_LEADS_DB"] = "production_raw_dump/leads-db"
        os.environ["S3_PREFIX_CRM"] = "production_raw_dump/crm"
        assert resolve_s3_prefix("leads-db") == "production_raw_dump/leads-db"
        assert resolve_s3_prefix("leads_db") == "production_raw_dump/leads-db"
        assert resolve_s3_prefix("crm") == "production_raw_dump/crm"
        assert (
            resolve_s3_prefix(s3_prefix="production_raw_dump/other")
            == "production_raw_dump/other"
        )

        os.environ.pop("S3_PREFIX_LEADS_DB")
        os.environ["S3_PREFIX"] = "production_raw_dump/leads-db"
        assert resolve_s3_prefix("leads-db") == "production_raw_dump/leads-db"
        # CRM without named key falls back to production_raw_dump/crm (not legacy leads)
        assert resolve_s3_prefix("crm") == "production_raw_dump/crm"

        print("ok")
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
