import os
import unittest
from unittest.mock import Mock, patch

from dataSource.engines.base import BaseLoader
from dataSource.engines.duckdb_engine import DuckDBLoader
from dataSource.engines.athena_engine import AthenaLoader
from dataSource.engines.wrangler_engine import WranglerLoader
from dataSource.exceptions import ConfigurationError
from dataSource.factory import DataLoader, LoaderFactory


class _FakeLoader(BaseLoader):
    engine = "fake"

    def __init__(self, **config):
        self.config = config
        self.calls = []

    def read_parquet(self, path=None, *, columns=None, **kwargs):
        self.calls.append(("read_parquet", path, columns))
        return "parquet"

    def read_sql(self, sql, **kwargs):
        self.calls.append(("read_sql", sql))
        return "sql"

    def read_table(self, table, *, columns=None, date_column=None,
                   start_date=None, end_date=None, **kwargs):
        self.calls.append(("read_table", table, columns))
        return "table"

    def scan(self, target, **kwargs):
        self.calls.append(("scan", target))
        return iter([])


class LoaderFactoryTest(unittest.TestCase):
    def test_creates_duckdb_loader(self):
        loader = LoaderFactory.create("duckdb")
        self.assertIsInstance(loader, DuckDBLoader)

    def test_engine_name_is_case_insensitive(self):
        loader = LoaderFactory.create("DuckDB")
        self.assertIsInstance(loader, DuckDBLoader)

    def test_creates_athena_loader_with_config(self):
        loader = LoaderFactory.create(
            "athena",
            database="analytics",
            workgroup="primary",
            output_location="s3://results/",
        )
        self.assertIsInstance(loader, AthenaLoader)

    def test_creates_wrangler_loader(self):
        loader = LoaderFactory.create("wrangler", bucket="bkt")
        self.assertIsInstance(loader, WranglerLoader)

    def test_invalid_engine_raises_with_supported_list(self):
        with self.assertRaises(ConfigurationError) as ctx:
            LoaderFactory.create("snowflake")
        message = str(ctx.exception)
        self.assertIn("snowflake", message)
        for name in ("athena", "duckdb", "wrangler"):
            self.assertIn(name, message)

    def test_empty_engine_raises(self):
        with self.assertRaises(ValueError):
            LoaderFactory.create("  ")

    def test_supported_engines_sorted(self):
        engines = LoaderFactory.supported_engines()
        self.assertEqual(engines, tuple(sorted(engines)))
        for name in ("athena", "duckdb", "wrangler"):
            self.assertIn(name, engines)

    def test_register_custom_engine(self):
        LoaderFactory.register("fake", _FakeLoader)
        try:
            loader = LoaderFactory.create("fake", token="t")
            self.assertIsInstance(loader, _FakeLoader)
            self.assertEqual(loader.config, {"token": "t"})
        finally:
            del LoaderFactory._registry["fake"]

    def test_register_duplicate_requires_overwrite(self):
        LoaderFactory.register("fake", _FakeLoader)
        try:
            with self.assertRaises(ConfigurationError):
                LoaderFactory.register("fake", _FakeLoader)
            LoaderFactory.register("fake", _FakeLoader, overwrite=True)
        finally:
            del LoaderFactory._registry["fake"]

    def test_register_rejects_non_loader(self):
        with self.assertRaises(ConfigurationError):
            LoaderFactory.register("bogus", object)  # type: ignore[arg-type]


class AthenaConfigurationTest(unittest.TestCase):
    def test_missing_database_raises(self):
        with patch.dict(os.environ, {"ATHENA_DATABASE": ""}):
            with self.assertRaises(ConfigurationError):
                AthenaLoader()

    def test_env_fallback_used(self):
        env = {
            "ATHENA_DATABASE": "envdb",
            "ATHENA_WORKGROUP": "envwg",
            "ATHENA_OUTPUT_LOCATION": "s3://env-results/",
        }
        with patch.dict(os.environ, env):
            loader = AthenaLoader()
        self.assertEqual(loader._database, "envdb")
        self.assertEqual(loader._workgroup, "envwg")
        self.assertEqual(loader._output_location, "s3://env-results/")


class DataLoaderTest(unittest.TestCase):
    def setUp(self):
        LoaderFactory.register("fake", _FakeLoader)

    def tearDown(self):
        del LoaderFactory._registry["fake"]

    def test_default_engine_is_duckdb(self):
        self.assertEqual(DataLoader().engine_name, "duckdb")
        self.assertIsInstance(DataLoader().backend, DuckDBLoader)

    def test_delegates_all_methods_to_backend(self):
        dl = DataLoader(engine="fake")
        backend = dl.backend

        self.assertEqual(dl.read_parquet("s3://b/p/", columns=["a"]), "parquet")
        self.assertEqual(dl.read_sql("SELECT 1"), "sql")
        self.assertEqual(dl.read_table("t", columns=["a"]), "table")
        self.assertEqual(dl.load_dataframe("t"), "table")
        list(dl.scan("target"))

        kinds = [c[0] for c in backend.calls]
        self.assertEqual(
            kinds, ["read_parquet", "read_sql", "read_table", "read_table", "scan"]
        )

    def test_invalid_engine_raises(self):
        with self.assertRaises(ConfigurationError):
            DataLoader(engine="not-an-engine")


if __name__ == "__main__":
    unittest.main()
