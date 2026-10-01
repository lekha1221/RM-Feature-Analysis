import os
import unittest
from unittest.mock import patch

from dataSource.aws_auth import configure_duckdb_s3, resolve_aws_auth


class _FrozenCredentials:
    def __init__(self, access_key, secret_key, token=None):
        self.access_key = access_key
        self.secret_key = secret_key
        self.token = token


class _Credentials:
    def __init__(self, access_key, secret_key, token=None):
        self._frozen = _FrozenCredentials(access_key, secret_key, token)

    def get_frozen_credentials(self):
        return self._frozen


class _Session:
    region_name = None

    def __init__(self, credentials):
        self.credentials = credentials
        self.static_values_seen = None

    def get_credentials(self):
        self.static_values_seen = (
            os.getenv("AWS_ACCESS_KEY_ID"),
            os.getenv("AWS_SECRET_ACCESS_KEY"),
            os.getenv("AWS_SESSION_TOKEN"),
        )
        return self.credentials


class _Connection:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)


class AWSAuthTest(unittest.TestCase):
    def test_dotenv_wins_over_instance_role(self):
        """Static .env keys are preferred when present (GitHub / local .env)."""
        session = _Session(_Credentials("role-id", "role-secret", "role-token"))
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "dotenv-id",
                "AWS_SECRET_ACCESS_KEY": "dotenv-secret",
                "AWS_SESSION_TOKEN": "dotenv-token",
            },
            clear=True,
        ), patch("dataSource.aws_auth.boto3.Session", return_value=session) as session_cls:
            auth = resolve_aws_auth()

        session_cls.assert_not_called()
        self.assertEqual(auth.access_key_id, "dotenv-id")
        self.assertEqual(auth.session_token, "dotenv-token")
        self.assertIsNone(auth.session)

    def test_instance_role_used_when_no_static_keys(self):
        session = _Session(_Credentials("role-id", "role-secret", "role-token"))
        with patch.dict(os.environ, {}, clear=True), patch(
            "dataSource.aws_auth.boto3.Session", return_value=session
        ):
            auth = resolve_aws_auth()

        self.assertEqual(session.static_values_seen, (None, None, None))
        self.assertEqual(auth.access_key_id, "role-id")
        self.assertEqual(auth.session_token, "role-token")

    def test_akia_static_keys_drop_leftover_session_token(self):
        session = _Session(None)
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "AKIADOTEXAMPLE",
                "AWS_SECRET_ACCESS_KEY": "dotenv-secret",
                "AWS_SESSION_TOKEN": "stale-token",
            },
            clear=True,
        ), patch("dataSource.aws_auth.boto3.Session", return_value=session):
            auth = resolve_aws_auth()

        self.assertEqual(auth.access_key_id, "AKIADOTEXAMPLE")
        self.assertIsNone(auth.session_token)

    def test_no_credentials_cleanly_reports_none(self):
        session = _Session(None)
        with patch.dict(os.environ, {}, clear=True), patch(
            "dataSource.aws_auth.boto3.Session", return_value=session
        ):
            auth = resolve_aws_auth()

        self.assertFalse(auth.has_credentials)

    def test_static_fallback_configures_duckdb_key_and_secret(self):
        session = _Session(None)
        con = _Connection()
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "dotenv-id",
                "AWS_SECRET_ACCESS_KEY": "dotenv-secret",
            },
            clear=True,
        ), patch("dataSource.aws_auth.boto3.Session", return_value=session):
            configure_duckdb_s3(con)

        self.assertEqual(len(con.statements), 1)
        stmt = con.statements[0]
        self.assertIn("CREATE OR REPLACE SECRET s3dataloader_s3", stmt)
        self.assertIn("KEY_ID 'dotenv-id'", stmt)
        self.assertIn("SECRET 'dotenv-secret'", stmt)
        self.assertIn("SESSION_TOKEN ''", stmt)

    def test_duckdb_receives_session_token(self):
        session = _Session(_Credentials("role-id", "role-secret", "role-token"))
        con = _Connection()
        with patch.dict(os.environ, {}, clear=True), patch(
            "dataSource.aws_auth.boto3.Session", return_value=session
        ):
            configure_duckdb_s3(con)

        self.assertEqual(len(con.statements), 1)
        stmt = con.statements[0]
        self.assertIn("CREATE OR REPLACE SECRET s3dataloader_s3", stmt)
        self.assertIn("KEY_ID 'role-id'", stmt)
        self.assertIn("SECRET 'role-secret'", stmt)
        self.assertIn("SESSION_TOKEN 'role-token'", stmt)


if __name__ == "__main__":
    unittest.main()
