import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd

from dataSource.engines import wrangler_engine
from dataSource.engines.wrangler_engine import WranglerLoader
from dataSource.exceptions import ConfigurationError, WranglerError


def _fake_wr(read_parquet):
    return SimpleNamespace(s3=SimpleNamespace(read_parquet=read_parquet))


class WranglerLoaderTest(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "price": [10.0, 20.0, 30.0],
                "createdAt": pd.to_datetime(
                    ["2026-05-30", "2026-06-03", "2026-06-10"]
                ),
            }
        )
        self.read_parquet = Mock(return_value=self.df)
        self.loader = WranglerLoader(
            s3_prefix="prefix", bucket="bkt", boto3_session=Mock(name="session")
        )
        patcher = patch.object(
            wrangler_engine, "_import_awswrangler",
            return_value=_fake_wr(self.read_parquet),
        )
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_read_parquet_with_explicit_path(self):
        result = self.loader.read_parquet(
            path="s3://bucket/data/", columns=["id", "price"]
        )
        self.assertIs(result, self.df)
        kwargs = self.read_parquet.call_args.kwargs
        self.assertEqual(kwargs["path"], "s3://bucket/data/")
        self.assertEqual(kwargs["columns"], ["id", "price"])
        self.assertTrue(kwargs["dataset"])
        self.assertEqual(kwargs["path_suffix"], ".parquet")

    def test_read_parquet_with_path_list(self):
        paths = ["s3://bucket/a.parquet", "s3://bucket/b.parquet"]
        self.loader.read_parquet(path=paths)
        self.assertEqual(self.read_parquet.call_args.kwargs["path"], paths)

    def test_read_parquet_collection_resolves_prefix(self):
        self.loader.read_parquet(collection="contacts")
        self.assertEqual(
            self.read_parquet.call_args.kwargs["path"], "s3://bkt/prefix/contacts/"
        )

    def test_read_parquet_indexed_collection_uses_data_dir(self):
        self.loader.read_parquet(collection="visitorlogs")
        self.assertEqual(
            self.read_parquet.call_args.kwargs["path"],
            "s3://bkt/prefix/visitorlogs/data/",
        )

    def test_read_parquet_requires_exactly_one_target(self):
        with self.assertRaises(ValueError):
            self.loader.read_parquet()
        with self.assertRaises(ValueError):
            self.loader.read_parquet(path="s3://bucket/", collection="contacts")

    def test_read_parquet_rejects_non_s3_path(self):
        with self.assertRaises(ValueError):
            self.loader.read_parquet(path="/local/data/")

    def test_collection_requires_bucket(self):
        loader = WranglerLoader(s3_prefix="prefix", bucket=" ", boto3_session=Mock())
        with patch.dict("os.environ", {"S3_BUCKET": ""}):
            with self.assertRaises(ConfigurationError):
                loader.read_parquet(collection="contacts")

    def test_read_parquet_passes_partition_filter_and_chunked(self):
        predicate = lambda p: p.get("year") == "2026"  # noqa: E731
        self.read_parquet.return_value = iter([self.df])
        self.loader.read_parquet(
            path="s3://bucket/data/", partition_filter=predicate, chunked=True
        )
        kwargs = self.read_parquet.call_args.kwargs
        self.assertIs(kwargs["partition_filter"], predicate)
        self.assertTrue(kwargs["chunked"])

    def test_read_parquet_wraps_backend_errors(self):
        self.read_parquet.side_effect = RuntimeError("s3 down")
        with self.assertRaises(WranglerError) as ctx:
            self.loader.read_parquet(path="s3://bucket/data/")
        self.assertIn("s3 down", str(ctx.exception))

    def test_read_sql_not_supported(self):
        with self.assertRaises(WranglerError) as ctx:
            self.loader.read_sql("SELECT 1")
        self.assertIn("athena", str(ctx.exception))

    def test_read_table_filters_dates_inclusively_client_side(self):
        df = self.loader.read_table(
            "contacts",
            columns=["id"],
            date_column="createdAt",
            start_date="2026-06-01",
            end_date="2026-06-07",
        )
        # createdAt requested internally for filtering, then dropped
        self.assertEqual(
            self.read_parquet.call_args.kwargs["columns"], ["id", "createdAt"]
        )
        self.assertEqual(list(df.columns), ["id"])
        self.assertEqual(df["id"].tolist(), [2])

    def test_read_table_without_dates_returns_frame_unchanged(self):
        df = self.loader.read_table("contacts")
        self.assertIs(df, self.df)
        self.assertNotIn("columns", self.read_parquet.call_args.kwargs)

    def test_read_table_partial_date_filter_raises(self):
        with self.assertRaises(ValueError):
            self.loader.read_table("contacts", date_column="createdAt")

    def test_read_table_unknown_date_column_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self.loader.read_table(
                "contacts",
                date_column="missing",
                start_date="2026-06-01",
                end_date="2026-06-07",
            )
        self.assertIn("Available columns", str(ctx.exception))

    def test_scan_collection_and_path_targets(self):
        chunks = iter([self.df])
        self.read_parquet.return_value = chunks

        result = self.loader.scan("contacts", chunked=1000)
        self.assertIs(result, chunks)
        kwargs = self.read_parquet.call_args.kwargs
        self.assertEqual(kwargs["path"], "s3://bkt/prefix/contacts/")
        self.assertEqual(kwargs["chunked"], 1000)

        self.read_parquet.return_value = iter([self.df])
        self.loader.scan("s3://bucket/data/")
        self.assertEqual(
            self.read_parquet.call_args.kwargs["path"], "s3://bucket/data/"
        )


if __name__ == "__main__":
    unittest.main()
