# ADR: Athena → Python Data Loading for s3DataLoader

**Status:** Proposed  
**Scope:** Best long-term libraries/architecture for loading Amazon Athena results into Python (pandas / Polars / PyArrow), in the context of s3DataLoader v2.0.0

---

## 1. Executive Summary

### Problem

DuckDB-as-default for direct S3/Parquet lake scans can generate excessive GET traffic (team observation ~1,000 GETs/minute; community cases report ~150k GETs for a single wide Parquet file), causing throttling, slowdowns, and unreliable production loads. We need an **Athena-first** standard for Python result materialisation.

### Key findings

1. **Result architecture matters more than brand of client.** Two paths exist:
  - **Slow:** Athena `GetQueryResults` API (hard cap **1,000 rows per call** — [AWS API docs](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html))
  - **Fast:** Read Athena output from **S3** (CSV, or Parquet via **CTAS** / **UNLOAD**)
2. **AWS SDK for pandas (**`awswrangler`**)** is the strongest production default for Athena → pandas, with official CTAS ~**13×** and categories ~**24×** vs CSV-from-S3 ([Wrangler Athena tutorial](https://aws-sdk-pandas.readthedocs.io/en/stable/tutorials/006%20-%20Amazon%20Athena.html)).
3. **PyAthena** with `PandasCursor`/`ArrowCursor`/`PolarsCursor` + `unload=True` is the best open-source DBAPI alternative (async, SQLAlchemy, Polars). Default cursor without UNLOAD is the slow path.
4. **Metabase API**, **JDBC/ODBC**, and raw **GetQueryResults** pagination are unsuitable as s3DataLoader bulk engines.
5. **DuckDB** remains excellent for small/local analysis; not a production lake-scan default.



### Final recommendation


| Decision                      | Choice                                                                               |
| ----------------------------- | ------------------------------------------------------------------------------------ |
| Default Athena engine         | **awswrangler** (`unload_approach` preferred; `ctas_approach` when Glue DDL allowed) |
| Bulk Parquet (known S3 paths) | **awswrangler** `s3.read_parquet` (unchanged)                                        |
| Optional second Athena engine | **PyAthena** (UNLOAD + Pandas/Polars/async)                                          |
| DuckDB                        | **Keep**, demote to ad-hoc / small only                                              |
| Metabase / JDBC as loader     | **Do not adopt**                                                                     |




### Top three solutions


| Rank | Solution                                | Score /100 | Primary strength                  | Primary weakness                    |
| ---- | --------------------------------------- | ---------- | --------------------------------- | ----------------------------------- |
| 1    | awswrangler (CTAS / UNLOAD / S3)        | 92         | S3-direct Parquet + AWS-native DX | CTAS needs Glue create/delete       |
| 2    | PyAthena (UNLOAD + Pandas/Arrow/Polars) | 86         | DBAPI, async, SQLAlchemy, Polars  | Easy to misconfigure default cursor |
| 3    | Boto3 + UNLOAD + PyArrow/pandas         | 72         | Full control, minimal deps        | Re-implements Wrangler/PyAthena     |




### Quick decision table


| Workload                    | Preferred                               | Fallback               |
| --------------------------- | --------------------------------------- | ---------------------- |
| Large production SQL (Glue) | awswrangler UNLOAD/CTAS                 | PyAthena `unload=True` |
| Bulk Parquet from S3        | awswrangler `read_parquet`              | PyArrow dataset        |
| Notebooks / DS              | awswrangler or PyAthena                 | DuckDB (bounded)       |
| Backend APIs                | awswrangler + `chunksize`               | PyAthena async         |
| SQLAlchemy apps             | PyAthena + SQLAlchemy                   | —                      |
| 100M+ row materialisation   | UNLOAD → process Parquet; avoid full DF | Spark / EMR            |


---



## 2. Problem Statement



### Why this research

s3DataLoader v2.0.0 supports multiple engines (Athena, AWS Wrangler, DuckDB). Leadership requested comprehensive R&D on whether Athena loading should standardise on Wrangler, PyAthena, Boto3, Metabase, or another production-ready approach - and what becomes the long-term default.

### Current limitations

- No single evidence-backed default for Athena → DataFrame
- Risk of DuckDB as silent default on large partitioned lakes
- Teams may accidentally use Results API pagination
- Unclear guidance on CTAS vs UNLOAD vs CSV



### Why DuckDB is no longer preferred for production lake scans

DuckDB `httpfs` can issue extreme S3 GET volume when scanning Parquet with many row groups × columns ([duckdb-httpfs#172](https://github.com/duckdb/duckdb-httpfs/issues/172): ~150k requests / ~19 minutes vs Arrow ~60 requests / ~1m20s for the same file). This matches the team’s throttling/reliability concerns.

### Why Athena-first

- Serverless SQL over Glue Catalog / Iceberg with partition pruning and predicate pushdown in-engine
- Workgroups, IAM, encryption, result reuse
- Moves heavy scanning off the client; client only materialises **query results**

---



## 3. Architecture

```
S3 Data Lake (Parquet / Iceberg / CSV)
        │
   Glue Catalog
        │
   Amazon Athena  (workgroup / result reuse / SELECT | UNLOAD | CTAS)
        │
   ┌────┴─────────────────────────────────────┐
   │         Result materialisation           │
   ├─ Fast: S3 result files (CSV / Parquet)   │
   │    awswrangler · PyAthena UNLOAD         │
   │    Boto3 + PyArrow · Polars scan_parquet │
   ├─ Slow: GetQueryResults (≤1000 rows/page) │
   │    raw boto3 · PyAthena default Cursor   │
   └─ Avoid as loader: Metabase · JDBC/ODBC   │
        │
   s3DataLoader engines → Python
        │
   pandas / Polars / PyArrow
```


| Library          | Role                                            |
| ---------------- | ----------------------------------------------- |
| awswrangler      | Primary Athena + S3 engines                     |
| PyAthena         | Optional Athena engine (DBAPI / async / Polars) |
| Boto3            | Internal control-plane primitive                |
| DuckDB           | Ad-hoc / small only                             |
| PyArrow / Polars | Complements after UNLOAD                        |
| Metabase / JDBC  | BI only — out of loader core                    |


---



## 4. Evaluation Methodology


| Category         | Weight | Why                                 |
| ---------------- | ------ | ----------------------------------- |
| Performance      | 25%    | Latency & memory dominate SLOs      |
| Scalability      | 20%    | Must span <1M → 100M+ without cliff |
| Ease of use      | 15%    | DS + backend velocity               |
| AWS integration  | 15%    | IAM, SSO, Glue, workgroups          |
| Maintainability  | 10%    | Ownership cost in s3DataLoader      |
| Cost             | 10%    | Scanned bytes + S3 + temp storage   |
| Community & docs | 5%     | Debug / onboarding                  |


**Evidence tiers:** Official (AWS / library docs) · Third-party benchmark · Production/community report.

---



## 5. Feature Comparison Matrix


| Capability     | Wrangler     | PyAthena | Boto3        | DuckDB  | Metabase | JDBC/ODBC | Trino client |
| -------------- | ------------ | -------- | ------------ | ------- | -------- | --------- | ------------ |
| Async          | Partial      | **Yes**  | Partial      | No      | No       | No        | Partial      |
| Chunking       | **Yes**      | **Yes**  | Partial      | Partial | No       | Partial   | Partial      |
| Streaming      | Partial      | Partial  | Partial      | Partial | No       | **Yes**   | Partial      |
| SQLAlchemy     | No           | **Yes**  | No           | No      | No       | Partial   | **Yes**      |
| Pandas         | **Yes**      | **Yes**  | Partial      | **Yes** | Partial  | Partial   | Partial      |
| Polars         | Partial      | **Yes**  | Partial      | **Yes** | No       | No        | Partial      |
| PyArrow        | **Yes**      | **Yes**  | Partial      | **Yes** | No       | No        | Partial      |
| Glue           | **Yes**      | **Yes**  | **Yes**      | Partial | Partial  | **Yes**   | Partial      |
| IAM / SSO      | **Yes**      | **Yes**  | **Yes**      | Partial | Partial  | **Yes**   | Partial      |
| Retry          | **Yes**      | **Yes**  | Partial      | Partial | Partial  | Partial   | Partial      |
| Pagination     | **Yes**      | **Yes**  | **Yes**      | No      | Partial  | **Yes**   | **Yes**      |
| Result caching | **Yes**      | **Yes**  | Partial      | No      | Partial  | Partial   | Partial      |
| Maintenance    | Active (AWS) | Active   | Active (AWS) | Active  | Active   | Vendor    | Active       |


 Predicate pushdown / partition pruning occur in **Athena** (or DuckDB/Trino), not in the Python client. Client choice mainly affects **result download**.

---



## 6. Performance Benchmark Comparison



### Relative architecture (normalised index; lower = better)


| Approach                   | Relative time index | Evidence                                                            |
| -------------------------- | ------------------- | ------------------------------------------------------------------- |
| GetQueryResults pagination | 100                 | AWS MaxResults=1000; production reports of thousands of round-trips |
| CSV from S3                | ~40                 | Wrangler / PyAthena PandasCursor baseline                           |
| UNLOAD Parquet             | ~12                 | Community ~4× vs CSV; PyAthena #242                                 |
| CTAS Parquet               | ~8                  | Wrangler docs ~13× vs CSV                                           |
| CTAS + categories          | ~4                  | Wrangler docs ~24× vs CSV                                           |




### By dataset size (guidance)


| Size               | Best approach                                        | Memory      | Notes                             |
| ------------------ | ---------------------------------------------------- | ----------- | --------------------------------- |
| Small (<1M)        | Wrangler CSV (`ctas=False`) or PyAthena PandasCursor | Low         | Query time dominates              |
| Medium (1–20M)     | Wrangler CTAS/UNLOAD or PyAthena `unload=True`       | Medium–high | Parallel Parquet read             |
| Large (20–100M)    | UNLOAD/CTAS + `chunksize`; avoid full DF             | Peak risk   | Must chunk                        |
| Very large (100M+) | UNLOAD to S3 then process; Spark/EMR                 | Must stream | Do not materialise full DataFrame |




### Cited measurements


| Type              | Comparison                                 | Result                             | Source                                                                                                              |
| ----------------- | ------------------------------------------ | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Official          | Wrangler CTAS vs CSV-from-S3               | ~13× faster                        | [aws-sdk-pandas tutorial](https://aws-sdk-pandas.readthedocs.io/en/stable/tutorials/006%20-%20Amazon%20Athena.html) |
| Official          | Wrangler CTAS + categories                 | ~24× faster                        | Same                                                                                                                |
| Third-party       | SQLAlchemy+PyAthena vs Wrangler (~2M rows) | 16m37s vs 1m34s                    | [PyAthena #254](https://github.com/pyathena-dev/PyAthena/issues/254) (SQLAlchemy was the bottleneck)                |
| Maintainer        | PandasCursor vs default Cursor (~1.9M)     | ~46s vs ~629s                      | [gist](https://gist.github.com/laughingman7743/2e4d83ca4e394dc645e9ea9a45fe78ba)                                    |
| Community         | UNLOAD vs CSV                              | ~4×; cached unload ~13×            | [PyAthena #242](https://github.com/pyathena-dev/PyAthena/issues/242)                                                |
| Production report | API pagination vs S3-direct (5M rows)      | ~5,000 API calls vs 1 S3 read path | Medium / Data Driven Investor 2025                                                                                  |
| Community         | DuckDB httpfs vs Arrow                     | ~150k GETs vs ~60                  | [duckdb-httpfs#172](https://github.com/duckdb/duckdb-httpfs/issues/172)                                             |


> Absolute wall-clock times vary by region, workgroup, and hardware. Prefer relative architecture conclusions; run an internal bake-off on company data before flipping production defaults.

---



## 7. Individual Deep Dive (summary)



### 7.1 awswrangler — **recommended default**

- **Overview:** Official AWS SDK for pandas (~4.1k★, active 2026 releases).
- **How:** CTAS→Parquet (default), UNLOAD→Parquet, or CSV-from-S3; also `wr.s3.read_parquet`.
- **Pros:** Fastest practical Athena→pandas; chunksize; Glue/S3 unified; enterprise familiarity.
- **Cons:** CTAS needs Glue DDL; some type limits on CTAS/UNLOAD; pandas-first.
- **When not:** SQLAlchemy-centric apps; no Glue DDL and CTAS required without UNLOAD validation.
- **s3DataLoader:** Promote as Athena default; prefer UNLOAD for least privilege.

```python
import awswrangler as wr

df = wr.athena.read_sql_query(
    "SELECT * FROM events WHERE dt = DATE '2026-07-01'",
    database="analytics",
    ctas_approach=False,
    unload_approach=True,
    s3_output="s3://bucket/athena-unload/",
    chunksize=True,
)
```



### 7.2 PyAthena — **best open-source / DBAPI**

- DBAPI 2.0 + Pandas/Arrow/Polars cursors + async + SQLAlchemy + SparkCursor.
- **Must** use specialised cursor + `unload=True` for large results.
- **s3DataLoader:** Optional secondary engine.

```python
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

conn = connect(s3_staging_dir="s3://bucket/athena/", region_name="eu-west-1")
cur = conn.cursor(PandasCursor, unload=True)
df = cur.execute("SELECT * FROM events WHERE dt = DATE '2026-07-01'").as_pandas()
```



### 7.3 Boto3

Foundation under both libraries. Fine as internal primitive; poor user-facing engine if you paginate `GetQueryResults`. Prefer StartQueryExecution → UNLOAD/OutputLocation → S3 Parquet read.

### 7.4 DuckDB

Keep for local/small. Document S3 GET risk. Not Athena substitute for production Glue SQL.

### 7.5 Metabase API

BI only. Interactive ~2k row cap; downloads ~1,048,575 row ceiling ([Metabase docs](https://www.metabase.com/docs/latest/questions/exporting-results)). **Reject** for loader core.

### 7.6 SQLAlchemy + Athena

Via PyAthena dialect — good for apps; historically slow when miswired ([#254](https://github.com/pyathena-dev/PyAthena/issues/254)). Not default bulk extract path.

### 7.7 JDBC / ODBC

BI tooling (Simba). Port 444 streaming. Weak Python DataFrame DX. Out of s3DataLoader default scope.

### 7.8 Trino client / Athena Spark

Trino client ≠ managed Athena. Athena for Apache Spark fits heavy ETL with DPU cost — special case only.

### 7.9 PyArrow / Polars / PyIceberg

Complements after UNLOAD. PyIceberg can bypass Athena for direct Iceberg scans — complementary, not full SQL replacement.

---



## 8. Decision Matrix


| Approach                         | Score  | Verdict           |
| -------------------------------- | ------ | ----------------- |
| awswrangler                      | **92** | **Default**       |
| PyAthena + UNLOAD                | **86** | **Add optional**  |
| Boto3 + UNLOAD + Arrow           | 72     | Internal only     |
| Athena Spark                     | 62     | Special case      |
| PyAthena default cursor          | 58     | Avoid for bulk    |
| Trino client (no Trino platform) | 50     | N/A unless Trino  |
| DuckDB httpfs lake scan          | 48     | Keep, demote      |
| JDBC/ODBC                        | 42     | Reject for loader |
| Boto3 GetQueryResults bulk       | 35     | Anti-pattern      |
| Metabase API                     | 28     | Reject            |


Weights: Performance 25 · Scalability 20 · Ease 15 · AWS 15 · Maintainability 10 · Cost 10 · Community 5.

---



## 9. Recommendation for s3DataLoader



### Per workload


| Use case                | Default engine                                     |
| ----------------------- | -------------------------------------------------- |
| Large production        | Athena via awswrangler UNLOAD/CTAS                 |
| Data science notebooks  | awswrangler (or PyAthena if Polars/SQLAlchemy)     |
| Interactive analysis    | Athena small results **or** DuckDB local (bounded) |
| Backend APIs            | awswrangler + chunksize + timeouts                 |
| ETL / batch / scheduled | awswrangler UNLOAD → Parquet stages                |
| High concurrency        | Workgroup limits + result reuse; avoid CTAS storms |




### Keep / add / change / deprecate


| Action               | Detail                                                                                     |
| -------------------- | ------------------------------------------------------------------------------------------ |
| **Keep**             | Multi-engine façade; Wrangler Athena + S3; DuckDB as explicit small engine                 |
| **Add**              | Optional PyAthena engine; approach selector `csv | unload | ctas`; safe chunksize defaults |
| **Change**           | Athena = preferred default for Glue SQL; auto-prefer UNLOAD for mid/large                  |
| **Deprecate (soft)** | DuckDB as silent default; GetQueryResults bulk paths; Metabase/JDBC as loader backends     |




### Migration


| Item          | Assessment                                                                   |
| ------------- | ---------------------------------------------------------------------------- |
| Effort        | Low–medium (config, docs, optional PyAthena adapter)                         |
| Compatibility | Preserve engine kwargs; change defaults with changelog + warnings            |
| Risks         | CTAS permission failures; UNLOAD empty path; timestamptz / nested type edges |
| Benefits      | Fewer S3 GET throttles; faster large pulls; clearer guidance                 |
| Validation    | Suite: CSV path, UNLOAD, CTAS, chunked ~10M, IAM-denied CTAS                 |


---



## 10. Final Conclusion


| Question                      | Answer                                                          | Evidence basis                                                  |
| ----------------------------- | --------------------------------------------------------------- | --------------------------------------------------------------- |
| Overall best for s3DataLoader | **awswrangler (UNLOAD/CTAS)**                                   | Official speedups + AWS maintenance + S3-direct architecture    |
| Best AWS-native               | **awswrangler**                                                 | AWS-owned aws-sdk-pandas                                        |
| Best open-source DBAPI        | **PyAthena**                                                    | Active DBAPI/async/Polars/SQLAlchemy                            |
| Best perf ↔ DX balance        | **UNLOAD → Parquet + Wrangler or PyAthena**                     | Avoids Glue mutate (vs CTAS) while keeping Parquet speed        |
| Long-term choice              | **Athena-first, S3-direct materialisation; DuckDB ad-hoc only** | API limits, Wrangler docs, DuckDB httpfs reports, Metabase caps |




### Next step

Time-boxed internal bake-off on company data: same SQL at **1M / 10M / 50M** rows across Wrangler CSV, Wrangler UNLOAD, Wrangler CTAS, PyAthena UNLOAD — measure wall time, peak RSS, S3 GET count, and Athena cost — then flip production defaults.

### Primary citations

- [AWS SDK for pandas — Athena tutorial](https://aws-sdk-pandas.readthedocs.io/en/stable/tutorials/006%20-%20Amazon%20Athena.html)
- [Amazon Athena UNLOAD](https://docs.aws.amazon.com/athena/latest/ug/unload.html)
- [GetQueryResults API](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html)
- [AWS Big Data Blog — UNLOAD](https://aws.amazon.com/blogs/big-data/simplify-your-etl-and-ml-pipelines-using-the-amazon-athena-unload-feature/)
- [aws-sdk-pandas GitHub](https://github.com/aws/aws-sdk-pandas)
- [PyAthena](https://github.com/pyathena-dev/PyAthena)
- [DuckDB httpfs S3 GET issue](https://github.com/duckdb/duckdb-httpfs/issues/172)
- [Metabase export limits](https://www.metabase.com/docs/latest/questions/exporting-results)

