"""Resolve AWS credentials from .env keys, with instance-role fallback."""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

import boto3


_STATIC_CREDENTIAL_ENV_KEYS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
)
# Hide shared files so the instance-role probe only sees IMDS/container creds —
# not ~/.aws/credentials or AWS_PROFILE.
_SHARED_CREDENTIAL_ENV_KEYS = (
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_CONFIG_FILE",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
)
_ENV_LOCK = threading.RLock()
_DEFAULT_REGION = "ap-south-1"
_MISSING_AWS_FILE = os.path.join(os.path.dirname(__file__), ".no-such-aws-file")


@dataclass(frozen=True)
class AWSAuth:
    """AWS credentials ready for boto3 and DuckDB."""

    access_key_id: str | None
    secret_access_key: str | None
    session_token: str | None
    region: str
    session: boto3.Session | None = None

    @property
    def has_credentials(self) -> bool:
        return bool(self.access_key_id and self.secret_access_key)


@contextmanager
def _without_static_credentials() -> Iterator[None]:
    """Temporarily hide dotenv/static keys while probing instance roles."""
    with _ENV_LOCK:
        saved = {key: os.environ.get(key) for key in _STATIC_CREDENTIAL_ENV_KEYS}
        try:
            for key in _STATIC_CREDENTIAL_ENV_KEYS:
                os.environ.pop(key, None)
            yield
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


@contextmanager
def _without_shared_credential_files() -> Iterator[None]:
    """Temporarily hide profiles / ~/.aws so only instance roles remain."""
    with _ENV_LOCK:
        saved = {key: os.environ.get(key) for key in _SHARED_CREDENTIAL_ENV_KEYS}
        # Keep IMDS probes snappy on laptops where no role exists.
        timeout_keys = {
            "AWS_EC2_METADATA_SERVICE_TIMEOUT": "1",
            "AWS_EC2_METADATA_SERVICE_NUM_ATTEMPTS": "1",
        }
        timeout_saved = {key: os.environ.get(key) for key in timeout_keys}
        try:
            os.environ["AWS_SHARED_CREDENTIALS_FILE"] = _MISSING_AWS_FILE
            os.environ["AWS_CONFIG_FILE"] = _MISSING_AWS_FILE
            os.environ.pop("AWS_PROFILE", None)
            os.environ.pop("AWS_DEFAULT_PROFILE", None)
            for key, value in timeout_keys.items():
                os.environ.setdefault(key, value)
            yield
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            for key, value in timeout_saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def _resolve_region(region: str | None, session: boto3.Session | None = None) -> str:
    return (
        region
        or (session.region_name if session is not None else None)
        or os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or _DEFAULT_REGION
    )


def _auth_from_session(session: boto3.Session, region: str | None) -> AWSAuth | None:
    credentials = session.get_credentials()
    if credentials is None:
        return None
    frozen = credentials.get_frozen_credentials()
    return AWSAuth(
        access_key_id=frozen.access_key,
        secret_access_key=frozen.secret_key,
        session_token=frozen.token,
        region=_resolve_region(region, session),
        session=session,
    )


def _fallback_static_credentials() -> tuple[str | None, str | None, str | None]:
    """Return complete static credentials from the environment, if available."""
    access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not access_key_id or not secret_access_key:
        return None, None, None
    session_token = os.getenv("AWS_SESSION_TOKEN") or os.getenv("AWS_SECURITY_TOKEN")
    # Permanent IAM user keys (AKIA…) never carry a session token. A leftover
    # AWS_SESSION_TOKEN in .env causes InvalidToken / SignatureDoesNotMatch.
    if access_key_id.startswith("AKIA"):
        session_token = None
    return access_key_id, secret_access_key, session_token


def resolve_aws_auth(region: str | None = None) -> AWSAuth:
    """Resolve credentials: ``.env`` static keys first, then instance role.

    GitHub users put keys in ``.env``. Local ``AWS_PROFILE`` / ``~/.aws`` are
    never consulted so a laptop profile cannot shadow project credentials.
    Instance/container roles are used only when static keys are absent.
    """
    # 1. Project .env / static environment keys
    access_key_id, secret_access_key, session_token = _fallback_static_credentials()
    if access_key_id and secret_access_key:
        return AWSAuth(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            region=_resolve_region(region),
        )

    # 2. Instance / container / web-identity role (ignore dotenv + ~/.aws)
    with _without_static_credentials(), _without_shared_credential_files():
        role_auth = _auth_from_session(boto3.Session(region_name=region), region)
        if role_auth is not None:
            return role_auth

    return AWSAuth(
        access_key_id=None,
        secret_access_key=None,
        session_token=None,
        region=_resolve_region(region),
    )


def create_s3_client(region: str | None = None):
    """Create an S3 client using .env keys or an instance role."""
    auth = resolve_aws_auth(region)
    if auth.session is not None:
        return auth.session.client("s3", region_name=auth.region)

    kwargs = {"region_name": auth.region}
    if auth.has_credentials:
        kwargs.update(
            aws_access_key_id=auth.access_key_id,
            aws_secret_access_key=auth.secret_access_key,
        )
        if auth.session_token:
            kwargs["aws_session_token"] = auth.session_token
    return boto3.client("s3", **kwargs)


def create_boto3_session(region: str | None = None) -> boto3.Session:
    """Create a boto3 Session using .env keys or an instance role.

    Mirrors :func:`create_s3_client` but returns the whole session, which is
    what awswrangler-based engines need (``boto3_session=...``).
    """
    auth = resolve_aws_auth(region)
    if auth.session is not None:
        return auth.session
    kwargs: dict = {"region_name": auth.region}
    if auth.has_credentials:
        kwargs.update(
            aws_access_key_id=auth.access_key_id,
            aws_secret_access_key=auth.secret_access_key,
        )
        if auth.session_token:
            kwargs["aws_session_token"] = auth.session_token
    return boto3.Session(**kwargs)


def configure_duckdb_s3(connection, region: str | None = None) -> AWSAuth:
    """Configure a DuckDB connection with the resolved S3 credentials.

    DuckDB 1.x authenticates S3 via the Secrets Manager (``CREATE SECRET``).
    Legacy ``SET s3_access_key_id`` / ``s3_session_token`` settings are ignored
    for auth, which leaves DuckDB on the env credential chain and commonly
    surfaces as ``InvalidToken`` when ``.env`` keys and a stale session token
    disagree.

    ``SESSION_TOKEN`` is always set explicitly (empty when unused) so a leftover
    ``AWS_SESSION_TOKEN`` in the process env cannot leak into DuckDB requests.
    """
    auth = resolve_aws_auth(region)

    def _esc(value: str) -> str:
        return value.replace("'", "''")

    # ponytail: CREATE OR REPLACE keeps the process-wide singleton connection safe to reconfigure
    parts = [
        "TYPE S3",
        "PROVIDER CONFIG",
        f"REGION '{_esc(auth.region)}'",
    ]
    if auth.has_credentials:
        parts.append(f"KEY_ID '{_esc(auth.access_key_id)}'")
        parts.append(f"SECRET '{_esc(auth.secret_access_key)}'")
        token = auth.session_token or ""
        parts.append(f"SESSION_TOKEN '{_esc(token)}'")

    connection.execute(f"CREATE OR REPLACE SECRET s3dataloader_s3 ({', '.join(parts)})")
    return auth
