import io
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd

from dataSource.hugeCollection_chunk_reading import S3IndexedCollectionReader


class _Body(io.BytesIO):
    def iter_lines(self):
        return self.getvalue().splitlines()


class _Paginator:
    def __init__(self, contents):
        self.contents = contents
        self.calls = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        yield {"Contents": self.contents}


class _S3:
    def __init__(self, manifest, master_modified, index_bodies, index_objects):
        self.manifest = manifest
        self.master_modified = master_modified
        self.index_bodies = index_bodies
        self.paginator = _Paginator(index_objects)
        self.get_object_keys = []

    def get_object(self, *, Bucket, Key):
        self.get_object_keys.append(Key)
        if Key.endswith("_index.json"):
            return {
                "Body": _Body(json.dumps(self.manifest).encode()),
                "LastModified": self.master_modified,
            }
        return {"Body": _Body(self.index_bodies[Key])}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self.paginator


class ChunkIndexReconciliationTest(unittest.TestCase):
    def _reader(self, s3):
        with patch(
            "dataSource.hugeCollection_chunk_reading.boto3.client", return_value=s3
        ):
            return S3IndexedCollectionReader(
                bucket="bucket",
                database="leads-db",
                collection="visitorlogs",
                s3_prefix="prefix",
            )

    def test_includes_chunk_missing_from_master_for_date_window(self):
        master_modified = datetime(2026, 6, 22, tzinfo=timezone.utc)
        base = "prefix/visitorlogs/_index"
        index_key = f"{base}/visitorlogs_chunk_1.index.jsonl"
        manifest = {
            "chunks": [
                {
                    "chunk_index": 0,
                    "parquet_key": "prefix/visitorlogs/data/visitorlogs_chunk_0.parquet",
                    "date_ranges": {
                        "createdAt": {
                            "min": "2026-06-01T00:00:00",
                            "max": "2026-06-22T00:00:00",
                        }
                    },
                }
            ]
        }
        s3 = _S3(
            manifest,
            master_modified,
            {index_key: b'{"createdAt":"2026-07-14T06:56:07"}\n'},
            [
                {
                    "Key": f"{base}/visitorlogs_chunk_0.index.jsonl",
                    "LastModified": datetime(2026, 6, 1, tzinfo=timezone.utc),
                },
                {
                    "Key": index_key,
                    "LastModified": datetime(2026, 7, 14, tzinfo=timezone.utc),
                },
            ],
        )

        chunks = self._reader(s3)._chunks_overlapping(
            "createdAt",
            pd.Timestamp("2026-07-14"),
            pd.Timestamp("2026-07-14 23:59:59"),
        )

        self.assertEqual([chunk["chunk_index"] for chunk in chunks], [1])
        self.assertEqual(
            chunks[0]["parquet_key"],
            "prefix/visitorlogs/data/visitorlogs_chunk_1.parquet",
        )
        self.assertEqual(
            s3.paginator.calls,
            [
                {
                    "Bucket": "bucket",
                    "Prefix": "prefix/visitorlogs/_index/visitorlogs_chunk_",
                }
            ],
        )
        self.assertEqual(
            s3.get_object_keys,
            ["prefix/visitorlogs/_index/visitorlogs_index.json", index_key],
        )

    def test_replaces_stale_master_date_range_with_newer_sidecar(self):
        master_modified = datetime(2026, 6, 22, tzinfo=timezone.utc)
        index_key = "prefix/visitorlogs/_index/visitorlogs_chunk_0.index.jsonl"
        manifest = {
            "chunks": [
                {
                    "chunk_index": 0,
                    "parquet_key": "prefix/visitorlogs/data/visitorlogs_chunk_0.parquet",
                    "date_ranges": {
                        "createdAt": {
                            "min": "2026-06-01T00:00:00",
                            "max": "2026-06-22T00:00:00",
                        }
                    },
                }
            ]
        }
        s3 = _S3(
            manifest,
            master_modified,
            {index_key: b'{"createdAt":"2026-07-14T06:56:07"}\n'},
            [
                {
                    "Key": index_key,
                    "LastModified": datetime(2026, 7, 14, tzinfo=timezone.utc),
                }
            ],
        )

        chunks = self._reader(s3)._chunks_overlapping(
            "createdAt",
            pd.Timestamp("2026-07-14"),
            pd.Timestamp("2026-07-14 23:59:59"),
        )

        self.assertEqual([chunk["chunk_index"] for chunk in chunks], [0])


if __name__ == "__main__":
    unittest.main()
