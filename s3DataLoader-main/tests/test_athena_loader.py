import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd

from dataSource.engines import athena_engine
from dataSource.engines.athena_engine import AthenaLoader
from dataSource.exceptions import AthenaError


def _fake_wr(read_sql_query):
    return SimpleNamespace(athena=SimpleNamespace(read_sql_query=read_sql_query))


class AthenaLoaderTest(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({"id": [1, 2]})
        self.read_sql_query = Mock(return_value=self.df)
        self.loader = AthenaLoader(
            database="analytics",
            workgroup="primary",
            output_location="s3://results/",
            max_cache_seconds=60,
            boto3_session=Mock(name="session"),
        )
        patcher = patch.object(
            athena_engine, "_import_awswrangler",
            return_value=_fake_wr(self.read_sql_query),
        )
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_read_sql_passes_configuration(self):
        result = self.loader.read_sql("SELECT * FROM sales LIMIT 100")

        self.assertIs(result, self.df)
        kwargs = self.read_sql_query.call_args.kwargs
        self.assertEqual(kwargs["sql"], "SELECT * FROM sales LIMIT 100")
        self.assertEqual(kwargs["database"], "analytics")
        self.assertEqual(kwargs["workgroup"], "primary")
        self.assertEqual(kwargs["s3_output"], "s3://results/")
        self.assertEqual(kwargs["max_cache_seconds"], 60)
        self.assertFalse(kwargs["ctas_approach"])
        self.assertNotIn("chunksize", kwargs)

    def test_read_sql_per_call_overrides(self):
        self.loader.read_sql(
            "SELECT 1", database="other", ctas_approach=True, chunksize=500
        )
        kwargs = self.read_sql_query.call_args.kwargs
        self.assertEqual(kwargs["database"], "other")
        self.assertTrue(kwargs["ctas_approach"])
        self.assertEqual(kwargs["chunksize"], 500)

    def test_read_sql_empty_raises(self):
        with self.assertRaises(ValueError):
            self.loader.read_sql("   ")

    def test_read_sql_wraps_backend_errors(self):
        self.read_sql_query.side_effect = RuntimeError("boom")
        with self.assertRaises(AthenaError) as ctx:
            self.loader.read_sql("SELECT 1")
        self.assertIn("boom", str(ctx.exception))

    def test_read_sql_retries_throttling(self):
        class _Throttle(Exception):
            response = {"Error": {"Code": "ThrottlingException"}}

        self.read_sql_query.side_effect = [_Throttle(), self.df]
        with patch.object(athena_engine, "_RETRY_BASE_DELAY", 0.0):
            result = self.loader.read_sql("SELECT 1")
        self.assertIs(result, self.df)
        self.assertEqual(self.read_sql_query.call_count, 2)

    def test_read_table_builds_projected_filtered_sql(self):
        self.loader.read_table(
            "sales",
            columns=["id", "price"],
            date_column="createdAt",
            start_date="2026-06-01",
            end_date="2026-06-07",
            limit=10,
        )
        sql = self.read_sql_query.call_args.kwargs["sql"]
        self.assertIn('SELECT "id", "price" FROM "sales"', sql)
        self.assertIn('"createdAt" >= TIMESTAMP \'2026-06-01 00:00:00\'', sql)
        self.assertIn('"createdAt" <= TIMESTAMP \'2026-06-07 00:00:00\'', sql)
        self.assertIn("LIMIT 10", sql)

    def test_read_table_qualified_name(self):
        self.loader.read_table("leads_analytics.facebook_ads_raw")
        sql = self.read_sql_query.call_args.kwargs["sql"]
        self.assertIn('FROM "leads_analytics"."facebook_ads_raw"', sql)

    def test_read_table_partial_date_filter_raises(self):
        with self.assertRaises(ValueError):
            self.loader.read_table("sales", date_column="createdAt")

    def test_read_table_reversed_dates_raise(self):
        with self.assertRaises(ValueError):
            self.loader.read_table(
                "sales",
                date_column="createdAt",
                start_date="2026-06-07",
                end_date="2026-06-01",
            )

    def test_read_table_rejects_quoted_identifiers(self):
        with self.assertRaises(ValueError):
            self.loader.read_table('sales" --')

    def test_read_parquet_not_supported(self):
        with self.assertRaises(AthenaError) as ctx:
            self.loader.read_parquet("s3://bucket/data/")
        self.assertIn("wrangler", str(ctx.exception))

    def test_scan_passes_chunksize(self):
        chunks = iter([self.df])
        self.read_sql_query.return_value = chunks
        result = self.loader.scan("SELECT * FROM big", chunksize=1000)
        self.assertIs(result, chunks)
        self.assertEqual(self.read_sql_query.call_args.kwargs["chunksize"], 1000)


if __name__ == "__main__":
    unittest.main()
