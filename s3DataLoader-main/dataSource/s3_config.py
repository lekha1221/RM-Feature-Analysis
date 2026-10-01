"""Resolve S3 dump prefixes for Lead-DB, CRM, or any ad-hoc path."""

from __future__ import annotations

import os

# Canonical source names → env-key suffix (S3_PREFIX_{SUFFIX}).
_SOURCE_ALIASES: dict[str, str] = {
    "leads-db": "LEADS_DB",
    "crm": "CRM",
}

_KNOWN_SOURCES = ("leads-db", "crm")


def normalize_source(source: str) -> str:
    """Map aliases to a canonical source name (leads-db | crm | raw)."""
    key = source.strip().lower()
    if key in ("leads-db", "leads_db", "leadsdb"):
        return "leads-db"
    if key == "crm":
        return "crm"
    return key


def resolve_s3_prefix(
    source: str | None = None,
    s3_prefix: str | None = None,
) -> str:
    """
    Resolve an S3 collection root prefix.

    Priority: explicit ``s3_prefix`` → named env ``S3_PREFIX_{SOURCE}``
    → legacy ``S3_PREFIX`` (leads-db only) → ``production_raw_dump/{source}``.
    """
    if s3_prefix is not None and str(s3_prefix).strip():
        return str(s3_prefix).strip().rstrip("/")

    if source is None or not str(source).strip():
        legacy = (os.getenv("S3_PREFIX") or "").strip().rstrip("/")
        if legacy:
            return legacy
        raise ValueError(
            "Pass source= (e.g. 'leads-db', 'crm') or s3_prefix=, "
            "or set S3_PREFIX / S3_PREFIX_LEADS_DB / S3_PREFIX_CRM in the environment."
        )

    canonical = normalize_source(str(source))
    suffix = _SOURCE_ALIASES.get(canonical)
    if suffix is None:
        named = (
            os.getenv(f"S3_PREFIX_{canonical.upper().replace('-', '_')}") or ""
        ).strip()
        if named:
            return named.rstrip("/")
        legacy = (os.getenv("S3_PREFIX") or "").strip().rstrip("/")
        if legacy:
            return legacy
        raise ValueError(
            f"Unknown source {source!r}. Known sources: {list(_KNOWN_SOURCES)}. "
            "Pass s3_prefix= for an arbitrary dump path, or set "
            f"S3_PREFIX_{canonical.upper().replace('-', '_')} in the environment."
        )

    named = (os.getenv(f"S3_PREFIX_{suffix}") or "").strip().rstrip("/")
    if named:
        return named

    legacy = (os.getenv("S3_PREFIX") or "").strip().rstrip("/")
    # ponytail: old .env only had S3_PREFIX pointing at leads-db
    if legacy and canonical == "leads-db":
        return legacy

    return f"production_raw_dump/{canonical}"
