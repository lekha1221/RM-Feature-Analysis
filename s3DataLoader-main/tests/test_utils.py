import unittest
from unittest.mock import Mock

from dataSource.utils import normalize_s3_path, require_non_empty_str, with_retries


class WithRetriesTest(unittest.TestCase):
    def test_succeeds_after_transient_failures(self):
        func = Mock(side_effect=[RuntimeError("a"), RuntimeError("b"), "ok"])
        wrapped = with_retries(attempts=3, base_delay=0.0)(func)
        self.assertEqual(wrapped(), "ok")
        self.assertEqual(func.call_count, 3)

    def test_raises_after_exhausting_attempts(self):
        func = Mock(side_effect=RuntimeError("always"))
        wrapped = with_retries(attempts=2, base_delay=0.0)(func)
        with self.assertRaises(RuntimeError):
            wrapped()
        self.assertEqual(func.call_count, 2)

    def test_retry_if_predicate_short_circuits(self):
        func = Mock(side_effect=RuntimeError("fatal"))
        wrapped = with_retries(
            attempts=3, base_delay=0.0, retry_if=lambda exc: False
        )(func)
        with self.assertRaises(RuntimeError):
            wrapped()
        self.assertEqual(func.call_count, 1)

    def test_invalid_attempts_raises(self):
        with self.assertRaises(ValueError):
            with_retries(attempts=0)


class ValidationTest(unittest.TestCase):
    def test_require_non_empty_str_strips(self):
        self.assertEqual(require_non_empty_str("  x  ", "v"), "x")

    def test_require_non_empty_str_rejects_blank_and_non_str(self):
        for bad in ("", "   ", None, 3):
            with self.assertRaises(ValueError):
                require_non_empty_str(bad, "v")

    def test_normalize_s3_path(self):
        self.assertEqual(normalize_s3_path(" s3://b/p/ "), "s3://b/p/")
        with self.assertRaises(ValueError):
            normalize_s3_path("/local/path")


if __name__ == "__main__":
    unittest.main()
