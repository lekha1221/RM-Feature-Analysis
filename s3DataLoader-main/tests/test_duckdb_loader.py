import unittest
from unittest.mock import Mock, patch

import pandas as pd

from dataSource.engines.duckdb_engine import DuckDBLoader, _FULL_SCAN_SQL
from dataSource.exceptions import ConfigurationError
from dataSource.getData import S3ParquetLoader


class _Result:
    """Fake DuckDB result yielding two chunks, then an empty frame."""

    def __init__(self):
        self._chunks = [
            pd.DataFrame({"n": [1, 2]}),
            pd.DataFrame({"n": [3]}),
            pd.DataFrame({"n": []}),
        ]

    def fetch_df_chunk(self):
        return self._chunks.pop(0)

    def fetchdf(self):
        return pd.DataFrame({"n": [1]})


class _Connection:
    def __init__(self):
        self.sql_seen = []
        self._result = _Result()

    def execute(self, sql):
        self.sql_seen.append(sql)
        return self._result


class DuckDBLoaderTest(unittest.TestCase):
    def setUp(self):
        self.loader = DuckDBLoader(s3_prefix="prefix", bucket="bkt")
        self.con = _Connection()
        patcher = patch.object(
            S3ParquetLoader, "_get_connection", return_value=self.con
        )
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_read_table_with_columns_delegates_to_loader(self):
        df = pd.DataFrame({"a": [1]})
        with patch.object(S3ParquetLoader, "loader", return_value=df) as mocked:
            result = self.loader.read_table(
                "leads",
                columns=["a"],
                date_column="createdAt",
                start_date="2026-06-01",
                end_date="2026-06-07",
            )
        self.assertIs(result, df)
        mocked.assert_called_once_with(
            "leads", ["a"], "createdAt", "2026-06-01", "2026-06-07"
        )

    def test_read_table_without_columns_uses_custom_loader(self):
        df = pd.DataFrame({"a": [1]})
        with patch.object(S3ParquetLoader, "custom_loader", return_value=df) as mocked:
            result = self.loader.read_table("leads")
        self.assertIs(result, df)
        args, kwargs = mocked.call_args
        self.assertEqual(args[0], _FULL_SCAN_SQL)
        self.assertEqual(args[1], "leads")

    def test_read_sql_with_collection_delegates_to_custom_loader(self):
        df = pd.DataFrame({"a": [1]})
        sql = "SELECT * FROM parquet_scan('{parquet_path}')"
        with patch.object(S3ParquetLoader, "custom_loader", return_value=df) as mocked:
            result = self.loader.read_sql(sql, collection="leads")
        self.assertIs(result, df)
        mocked.assert_called_once_with(
            sql, "leads", date_column=None, start_date=None, end_date=None
        )

    def test_read_sql_raw_runs_on_connection(self):
        result = self.loader.read_sql("SELECT 42 AS n")
        self.assertEqual(result["n"].tolist(), [1])
        self.assertEqual(self.con.sql_seen, ["SELECT 42 AS n"])

    def test_read_sql_placeholder_without_collection_raises(self):
        with self.assertRaises(ConfigurationError):
            self.loader.read_sql("SELECT * FROM parquet_scan('{parquet_path}')")

    def test_read_parquet_projects_columns(self):
        self.loader.read_parquet("s3://bkt/prefix/leads/*.parquet", columns=["a", "b"])
        sql = self.con.sql_seen[0]
        self.assertIn('SELECT "a", "b"', sql)
        self.assertIn("parquet_scan('s3://bkt/prefix/leads/*.parquet')", sql)

    def test_read_parquet_requires_string_path(self):
        with self.assertRaises(ValueError):
            self.loader.read_parquet(["s3://a", "s3://b"])
        with self.assertRaises(ValueError):
            self.loader.read_parquet(None)

    def test_scan_yields_chunks_until_empty(self):
        chunks = list(self.loader.scan("SELECT * FROM t"))
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0]["n"].tolist(), [1, 2])
        self.assertEqual(chunks[1]["n"].tolist(), [3])

    def test_scan_substitutes_collection_placeholder(self):
        list(
            self.loader.scan(
                "SELECT * FROM parquet_scan('{parquet_path}')", collection="leads"
            )
        )
        self.assertIn(
            "parquet_scan('s3://bkt/prefix/leads/*.parquet')", self.con.sql_seen[0]
        )

    def test_scan_placeholder_without_collection_raises(self):
        with self.assertRaises(ConfigurationError):
            self.loader.scan("SELECT * FROM parquet_scan('{parquet_path}')")

    def test_backward_compat_exports_unchanged(self):
        import dataSource

        self.assertIs(dataSource.S3ParquetLoader, S3ParquetLoader)
        self.assertIsInstance(self.loader.parquet_loader, S3ParquetLoader)


if __name__ == "__main__":
    unittest.main()
