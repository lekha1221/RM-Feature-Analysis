"""Read chunked, indexed S3 parquet collections (manifest + date-range pruning)."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Iterable, Sequence, Union

import duckdb
import pandas as pd
from dotenv import load_dotenv

from dataSource.aws_auth import configure_duckdb_s3, create_s3_client
from dataSource.s3_config import resolve_s3_prefix

load_dotenv()

DateLike = Union[str, pd.Timestamp, datetime]

# ponytail: list from data-engineering; add new indexed dumps here.
INDEXED_COLLECTIONS = frozenset(
    {
        "globalvisitorlogs",
        "whatsappchatmessages",
        "flowbuilderleadtransactions",
        "visitorlogs",
        "userredirecttracks",
    }
)

_PARQUET_SCAN_RE = re.compile(
    r"parquet_scan\s*\(\s*['\"]\{parquet_path\}['\"]\s*\)", re.IGNORECASE
)


def normalize_date_sql(value: DateLike, label: str) -> str:
    """Coerce a date/datetime to 'YYYY-MM-DD HH:MM:SS' for DuckDB SQL."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        raise ValueError(f"`{label}` cannot be None/NaN.")
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"`{label}` could not be parsed as a date: {value!r}")
    return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def require_full_date_filter(
    date_column: str | None,
    start_date: DateLike | None,
    end_date: DateLike | None,
) -> tuple[str, DateLike, DateLike] | None:
    """Return (date_column, start, end) when all three are set; else None or raise."""
    provided = (
        date_column is not None,
        start_date is not None,
        end_date is not None,
    )
    if all(provided):
        if not str(date_column).strip():
            raise ValueError("`date_column` must be a non-empty string.")
        return str(date_column).strip(), start_date, end_date
    if any(provided):
        raise ValueError(
            "Pass all of date_column, start_date, and end_date together, or omit all three."
        )
    return None


def wrap_sql_with_date_filter(
    sql_query: str,
    date_column: str,
    start_date: DateLike,
    end_date: DateLike,
) -> str:
    """Wrap SQL as a subquery and filter rows on ``date_column`` (inclusive)."""
    start_iso = normalize_date_sql(start_date, "start_date")
    end_iso = normalize_date_sql(end_date, "end_date")
    if start_iso > end_iso:
        raise ValueError(
            f"`start_date` ({start_iso}) must be <= `end_date` ({end_iso})."
        )
    inner = sql_query.strip().rstrip(";")
    return f"""
        SELECT * FROM ({inner}) AS _q
        WHERE "{date_column}" >= TIMESTAMP '{start_iso}'
          AND "{date_column}" <= TIMESTAMP '{end_iso}'
    """


class S3IndexedCollectionReader:
    """Manifest-aware reader for large S3 collections under ``<collection>/data/``."""

    def __init__(
        self,
        bucket: str,
        database: str,
        collection: str,
        region: str | None = None,
        *,
        s3_prefix: str | None = None,
    ) -> None:
        self.bucket = bucket
        self.database = database
        self.collection = collection
        self.region = region or os.getenv("AWS_REGION", "ap-south-1")
        # Explicit ctor prefix wins; else named env / legacy / production_raw_dump/{database}
        self.s3_prefix = resolve_s3_prefix(source=database, s3_prefix=s3_prefix)
        self._base = f"{self.s3_prefix}/{collection}"
        self._manifest: dict | None = None
        self._duck: duckdb.DuckDBPyConnection | None = None
        self._s3 = create_s3_client(self.region)

    @classmethod
    def from_env(
        cls,
        collection: str,
        database: str = "leads-db",
        *,
        s3_prefix: str | None = None,
    ) -> S3IndexedCollectionReader:
        bucket = os.getenv("S3_BUCKET", "airflow-etl-db")
        return cls(
            bucket=bucket,
            database=database,
            collection=collection,
            s3_prefix=s3_prefix,
        )

    def _manifest_key(self) -> str:
        return f"{self._base}/_index/{self.collection}_index.json"

    def load_manifest(self) -> dict:
        if self._manifest is None:
            obj = self._s3.get_object(Bucket=self.bucket, Key=self._manifest_key())
            self._manifest = json.loads(obj["Body"].read())
        return self._manifest

    def _get_duck(self) -> duckdb.DuckDBPyConnection:
        if self._duck is None:
            con = duckdb.connect()
            configure_duckdb_s3(con, self.region)
            self._duck = con
        return self._duck

    @staticmethod
    def _parse_date(value: DateLike) -> pd.Timestamp:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            raise ValueError(f"Could not parse date: {value!r}")
        return pd.Timestamp(ts).tz_localize(None)

    def describe_columns(self) -> list[str]:
        """Column names from the first manifest chunk (fast schema probe)."""
        chunks = self.load_manifest().get("chunks", [])
        if not chunks:
            raise ValueError(f"No chunks in manifest for {self.collection!r}.")
        path = f"s3://{self.bucket}/{chunks[0]['parquet_key']}"
        return (
            self._get_duck()
            .execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')")
            .df()["column_name"]
            .tolist()
        )

    def _chunks_overlapping(
        self, date_column: str, start: pd.Timestamp, end: pd.Timestamp
    ) -> list[dict]:
        selected: list[dict] = []
        for chunk in self.load_manifest().get("chunks", []):
            dr = chunk.get("date_ranges", {}).get(date_column)
            if not dr:
                selected.append(chunk)
                continue
            cmin = pd.to_datetime(dr["min"]).tz_localize(None)
            cmax = pd.to_datetime(dr["max"]).tz_localize(None)
            if cmax >= start and cmin <= end:
                selected.append(chunk)
        return selected

    def _parquet_paths(self, chunks: Iterable[dict]) -> list[str]:
        return [f"s3://{self.bucket}/{chunk['parquet_key']}" for chunk in chunks]

    @staticmethod
    def _parquet_from_clause(paths: Sequence[str]) -> str:
        if not paths:
            raise ValueError("No parquet paths to read.")
        if len(paths) == 1:
            return f"read_parquet('{paths[0]}')"
        joined = ", ".join(f"'{path}'" for path in paths)
        return f"read_parquet([{joined}])"

    def read_range(
        self,
        date_column: str | None = None,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
        columns: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        date_filter = require_full_date_filter(date_column, start_date, end_date)

        select = "*"
        if columns:
            select = ", ".join(f'"{col}"' for col in columns)

        if date_filter:
            dc, sd, ed = date_filter
            start = self._parse_date(sd)
            end = self._parse_date(ed)
            if start > end:
                raise ValueError("`start_date` must be <= `end_date`.")

            chunks = self._chunks_overlapping(dc, start, end)
            if not chunks:
                return pd.DataFrame()

            start_iso = normalize_date_sql(sd, "start_date")
            end_iso = normalize_date_sql(ed, "end_date")
            source = self._parquet_from_clause(self._parquet_paths(chunks))
            sql = f"""
                SELECT {select}
                FROM {source}
                WHERE "{dc}" >= TIMESTAMP '{start_iso}'
                  AND "{dc}" <= TIMESTAMP '{end_iso}'
            """
        else:
            # ponytail: no date window → glob all chunks
            source = f"read_parquet('{self._data_glob()}')"
            sql = f"SELECT {select} FROM {source}"

        return self._get_duck().execute(sql).fetchdf()

    def _data_glob(self) -> str:
        return f"s3://{self.bucket}/{self._base}/data/*.parquet"

    def read_sql(
        self,
        sql_query: str,
        *,
        date_column: str | None = None,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
    ) -> pd.DataFrame:
        if not sql_query.strip():
            raise ValueError("`sql_query` must be a non-empty string.")

        date_filter = require_full_date_filter(date_column, start_date, end_date)
        if date_filter:
            dc, sd, ed = date_filter
            chunks = self._chunks_overlapping(
                dc,
                self._parse_date(sd),
                self._parse_date(ed),
            )
            if not chunks:
                return pd.DataFrame()
            source = self._parquet_from_clause(self._parquet_paths(chunks))
        else:
            # ponytail: no date window → glob all chunks; lead/_id filters push down in DuckDB
            source = f"read_parquet('{self._data_glob()}')"

        sql = _PARQUET_SCAN_RE.sub(source, sql_query)
        if date_filter:
            dc, sd, ed = date_filter
            sql = wrap_sql_with_date_filter(sql, dc, sd, ed)
        return self._get_duck().execute(sql).fetchdf()


if __name__ == "__main__":
    sql = wrap_sql_with_date_filter(
        "SELECT 1 AS createdAt FROM parquet_scan('{parquet_path}')",
        "createdAt",
        "2026-06-01",
        "2026-06-03",
    )
    assert "WHERE" in sql and "_q" in sql
    assert require_full_date_filter("createdAt", "2026-06-01", "2026-06-03")
    assert require_full_date_filter(None, None, None) is None

    no_filter_sql = "SELECT 1 FROM read_parquet('s3://b/c/data/*.parquet')"
    assert "WHERE" not in no_filter_sql

    try:
        require_full_date_filter("createdAt", "2026-06-01", None)
        raise SystemExit("expected ValueError")
    except ValueError:
        pass
    print("ok")
