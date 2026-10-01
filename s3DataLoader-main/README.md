# S3 Parquet Loader

Load Airflow ETL parquet dumps from S3, returned as **pandas DataFrames**. All access goes through `dataSource/` — do not use ad-hoc boto3 / s3fs / pyarrow reads.

Supports three interchangeable engines behind one API — **DuckDB** (default, direct S3 reads), **Athena** (server-side SQL; preferred for large scans because S3 GET requests are rate-limited to ~1,000/minute), and **AWS Wrangler** (bulk S3 parquet reads) — plus **Lead-DB**, **CRM**, or **any S3 prefix** via named env keys and `source=` / `s3_prefix=`.

## Setup

```bash
cd project
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Optional — Athena and Wrangler engines
pip install "s3dataloader[aws]"
```

Copy `env.example` → `.env` and fill in S3 settings (**never commit `.env`**).
Local use reads credentials from `.env` (`AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`). On EC2 or Codespaces, leave those unset to use the
instance IAM role. No `AWS_PROFILE` / `~/.aws` is required for local use.

```env
S3_BUCKET=airflow-etl-db

# Named dump prefixes (preferred)
S3_PREFIX_LEADS_DB=production_raw_dump/leads-db
S3_PREFIX_CRM=production_raw_dump/crm

# Optional legacy fallback for leads-db when S3_PREFIX_LEADS_DB is unset
# S3_PREFIX=production_raw_dump/leads-db

AWS_REGION=ap-south-1

# Required for local use (your own keys). Omit on EC2/Codespaces with an instance role.
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
# AWS_SESSION_TOKEN=...   # only for temporary ASIA… keys; omit for AKIA…

# Athena engine defaults (optional)
# ATHENA_DATABASE=leads-prod
# ATHENA_WORKGROUP=asbl_analytics_prod
# ATHENA_OUTPUT_LOCATION=s3://airflow-etl-db/athena_storage/
```

Run Python from the **project root** so `dataSource` imports resolve:

```bash
cd project
python -c "from dataSource import S3ParquetLoader; print('ok')"
```

## Named sources

| `source=` | Env key | Typical prefix |
|-----------|---------|----------------|
| `"leads-db"` | `S3_PREFIX_LEADS_DB` | `production_raw_dump/leads-db` |
| `"crm"` | `S3_PREFIX_CRM` | `production_raw_dump/crm` |

Any other dump path: pass `s3_prefix=` directly (no named env key required).

```python
from dataSource import S3ParquetLoader

leads = S3ParquetLoader(source="leads-db")
crm = S3ParquetLoader(source="crm")
other = S3ParquetLoader(s3_prefix="production_raw_dump/some-other-dump")

# One loader, per-call override:
loader = S3ParquetLoader(source="leads-db")
df = loader.loader("contacts", ["_id"], source="crm")
```

`S3ParquetLoader()` with no args defaults to `source="leads-db"`.

## S3 layout

`{prefix}` is the resolved dump root (`S3_PREFIX_LEADS_DB`, `S3_PREFIX_CRM`, or `s3_prefix=`).

| Type | Path |
|------|------|
| Normal collection | `s3://{S3_BUCKET}/{prefix}/{collection}/*.parquet` |
| Large (indexed) | `s3://{S3_BUCKET}/{prefix}/{collection}/data/*.parquet` |
| Chunk manifest | `s3://{S3_BUCKET}/{prefix}/{collection}/_index/{collection}_index.json` |

Indexed collections (manifest + chunk pruning): `globalvisitorlogs`, `whatsappchatmessages`, `flowbuilderleadtransactions`, `visitorlogs`, `userredirecttracks`.

## Engines

`DataLoader` is the engine-agnostic entry point. Configure the engine once; the API (`read_parquet`, `read_sql`, `read_table`, `scan`, `load_dataframe`) is identical regardless of backend. The Athena and Wrangler engines need `awswrangler` (`pip install "s3dataloader[aws]"`).

```python
from dataSource import DataLoader

loader = DataLoader()                      # DuckDB, same defaults as before
loader = DataLoader(engine="wrangler")     # AWS Wrangler S3 reads

loader = DataLoader(                       # Athena (env vars ATHENA_DATABASE,
    engine="athena",                       # ATHENA_WORKGROUP, ATHENA_OUTPUT_LOCATION
    database="leads-prod",                 # are used as fallbacks)
    workgroup="asbl_analytics_prod",
    output_location="s3://airflow-etl-db/athena_storage/",
)

df = loader.read_sql("SELECT * FROM projects LIMIT 10")          # athena / duckdb
df = loader.read_table("leads", columns=["_id", "createdAt"],    # all engines
                       date_column="createdAt",
                       start_date="2025-06-01", end_date="2025-06-07")
df = loader.read_parquet(path="s3://bucket/data/", columns=["id"])  # wrangler / duckdb
for chunk in loader.scan("SELECT * FROM big_table"):             # chunked iteration
    ...
```

| Engine | Backed by | Best for | Not supported |
|--------|-----------|----------|---------------|
| `duckdb` (default) | DuckDB `parquet_scan` | Ad-hoc reads, indexed-collection chunk pruning | — |
| `athena` | `awswrangler.athena.read_sql_query` | Large scans, SQL over the Glue catalog (avoids S3 GET limits) | `read_parquet` |
| `wrangler` | `awswrangler.s3.read_parquet` | Bulk dataset reads, partition filters, chunked S3 reads | `read_sql` |

Unsupported operations raise the engine's error (`AthenaError`, `WranglerError`) with a pointer to the right engine; an unknown engine name raises `ConfigurationError` listing the supported engines. All errors subclass `LoaderError`. New engines can be plugged in via `LoaderFactory.register("snowflake", SnowflakeLoader)`.

The original `S3ParquetLoader` API below is unchanged and remains fully supported — `DataLoader()` with no arguments uses DuckDB with the same defaults.

## How to use

### 1. Simple load — columns, optional date range

```python
from dataSource import S3ParquetLoader

loader = S3ParquetLoader(source="leads-db")

# With date filter (inclusive range)
df = loader.loader(
    collection_name="leads",
    column_list=["_id", "createdAt", "phone"],
    date_column="createdAt",
    start_date="2025-06-01",
    end_date="2025-06-07",
)

# Without dates — full collection load
df = loader.loader(
    collection_name="leads",
    column_list=["_id", "createdAt", "phone"],
)
print(df.shape)
df.head()
```

- `date_column`, `start_date`, and `end_date` are **optional**. Pass all three together to filter rows, or omit all three for a full load.
- Passing only one or two of the three raises `ValueError`.
- `start_date` / `end_date`: strings, `datetime`, or `pandas.Timestamp` (inclusive). Prefer ISO `YYYY-MM-DD`.
- Missing columns are **skipped** with a console warning — check output if a column looks empty.
- When filtering, `date_column` must exist in the parquet schema or you get a `ValueError` listing available columns.
- Works for both flat and indexed collections — indexed ones use manifest chunk pruning automatically.

### 2. Custom SQL — aggregations, filters, joins-in-SQL

Use `{parquet_path}` as a placeholder; the loader substitutes the correct S3 glob or pruned chunk list.

```python
df = loader.custom_loader(
    sql_query="""
        SELECT leadId, COUNT(*) AS n
        FROM parquet_scan('{parquet_path}')
        WHERE status = 'active'
        GROUP BY leadId
    """,
    collection_name="somecollection",
)
```

For **indexed** collections, pass dates so only overlapping chunks are read:

```python
df = loader.custom_loader(
    sql_query="""
        SELECT leadId, message, createdAt
        FROM parquet_scan('{parquet_path}')
        WHERE message IS NOT NULL
    """,
    collection_name="whatsappchatmessages",
    date_column="createdAt",
    start_date="2025-06-01",
    end_date="2025-06-07",
)
```

Without dates on indexed collections, all chunks are globbed — fine for `WHERE leadId = '...'`, slow for full scans.

### 3. Indexed reader directly

```python
from dataSource import S3IndexedCollectionReader

reader = S3IndexedCollectionReader.from_env("visitorlogs", database="leads-db")

# With date filter (chunk pruning + row filter)
df = reader.read_range(
    date_column="createdAt",
    start_date="2025-06-01",
    end_date="2025-06-07",
    columns=["leadId", "createdAt", "url"],
)

# Without dates — glob all chunks (no WHERE)
df = reader.read_range(columns=["leadId", "createdAt", "url"])
```

Same all-or-none date rule as `loader()` / `custom_loader()`. Optional `s3_prefix=` on `from_env` for ad-hoc paths.

Custom SQL with chunk pruning:

```python
df = reader.read_sql(
    """
    SELECT leadId, COUNT(*) AS visits
    FROM parquet_scan('{parquet_path}')
    GROUP BY leadId
    """,
    date_column="createdAt",
    start_date="2025-06-01",
    end_date="2025-06-30",
)
```

No overlapping chunks → empty DataFrame (not an error).

### 4. Check which collections are indexed

```python
from dataSource import INDEXED_COLLECTIONS
print(sorted(INDEXED_COLLECTIONS))
```

Add new chunked dumps to `INDEXED_COLLECTIONS` in `dataSource/hugeCollection_chunk_reading.py` when data-engineering ships them.

## Tips

| Situation | Do this |
|-----------|---------|
| Local AWS auth | Put `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in `.env` (no `AWS_PROFILE` required) |
| Lead-DB vs CRM | `S3ParquetLoader(source="leads-db")` or `source="crm"` |
| Arbitrary dump | `S3ParquetLoader(s3_prefix="production_raw_dump/...")` |
| Large indexed collection | Pass `date_column` + `start_date` + `end_date`; omit all three only for a full scan |
| Date filter args | All three together or none — partial args raise `ValueError` |
| Ambiguous dates (`01/02/2025`) | Prefer ISO `YYYY-MM-DD` |
| Schema drift | `loader()` skips unknown columns; watch for `[loader] columns not found` |
| Large scans / S3 rate limits | `DataLoader(engine="athena")` for server-side SQL |
| Bulk parquet / partitions | `DataLoader(engine="wrangler")` |
| Catch all engine errors | `except LoaderError` |
| New indexed dump | Add name to `INDEXED_COLLECTIONS` and ensure `_index/{name}_index.json` exists on S3 |

## Project layout

```
├── .env                 # local only — do not commit
├── env.example
├── requirements.txt
├── pyproject.toml
├── getting_started.ipynb
└── dataSource/
    ├── __init__.py
    ├── aws_auth.py                       # .env keys → instance role
    ├── s3_config.py                      # resolve_s3_prefix (Lead-DB / CRM / any path)
    ├── getData.py                        # S3ParquetLoader
    ├── hugeCollection_chunk_reading.py   # S3IndexedCollectionReader, INDEXED_COLLECTIONS
    ├── factory.py                        # DataLoader, LoaderFactory
    ├── exceptions.py
    ├── utils.py
    └── engines/
        ├── duckdb_engine.py
        ├── athena_engine.py
        └── wrangler_engine.py
```

## Examples notebook

See [`getting_started.ipynb`](getting_started.ipynb) for runnable cells. The first cell validates `.env` S3 credentials before any demo loads. Replace `collection_name`, column names, and dates with values that exist in your S3 dumps.
