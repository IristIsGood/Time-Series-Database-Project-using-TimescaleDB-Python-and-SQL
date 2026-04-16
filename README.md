# Time-Series Database (TSDB) Project

**High-velocity, timestamped traffic detector data pipeline using TimescaleDB, Python, and SQL.**

This project demonstrates end-to-end handling of time-series data: from **ingestion** of simulation output through **storage** in a purpose-built **Time-Series Database**, to **analytical queries** including range scans, **downsampling**, **gap detection**, and **compression**. Built to showcase expertise in **high-write-throughput** scenarios and **time-centric** analytics.

---

## Project Architecture

High-level data flow from source to storage and analytics:

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│  SUMO Simulation    │     │  Python ETL          │     │  TimescaleDB        │
│  (detector_output   │ ──► │  • XML parsing       │ ──► │  • Hypertable       │
│   .xml)             │     │  • Timestamp mapping │     │  • Chunking (1 day) │
│  High-frequency     │     │  • Batch insert      │     │  • Compression      │
│  interval data      │     │    (1000 rows/batch) │     │  • Indexing         │
└─────────────────────┘     └─────────────────────┘     └──────────┬──────────┘
                                                                    │
                                                                    ▼
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│  Analytics &        │     │  SQL Layer          │     │  Stored Data        │
│  Reporting          │ ◄── │  • Range queries    │ ◄── │  detector_data      │
│  • Range stats      │     │  • time_bucket()    │     │  (time, detector_id,│
│  • Interpolation    │     │  • Window functions │     │   flow, speed, ...)  │
│  • Gap analysis     │     │  • Aggregates       │     │                     │
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘
```

| Stage | Component | Responsibility |
|-------|-----------|----------------|
| **Source** | SUMO detector XML | Emits high-velocity, **timestamped** interval data (flow, occupancy, speed). |
| **Ingestion** | `ingest_data_simple.py` | **ETL**: parse XML, normalize timestamps, **batch insert** into TSDB. |
| **Storage** | TimescaleDB (PostgreSQL) | **Hypertable** with **time-based chunking**, **compression**, **indexing**. |
| **Query** | `query_data_simple.py` | **SQL** for range queries, **downsampling**, **window functions**; **Python** for interpolation and orchestration. |

---

## Core Technical Competencies

Skills demonstrated in this project (bolded for quick scanning):

| Category | Competencies |
|----------|--------------|
| **Time-Series Storage** | **Hypertables**, **Chunking** (time-based partitioning), **TIMESTAMPTZ**-centric schema |
| **Querying** | **Time-Series Aggregation** (`time_bucket`), **Window Functions** (`LAG`), **Range Queries** (time-bounded `WHERE`) |
| **Efficiency** | **Downsampling** (e.g. 5-minute buckets), **Data Retention Policies**, **Compression** (segment-by + order-by) |
| **Performance** | **Indexing for Time-Centric Queries** (composite `detector_id`, `time DESC`), **Batch Inserts**, **Chunk Pruning** |
| **Data Quality** | **Gap Detection** (time gaps via window functions), **Interpolation** (linear, in Python) |
| **Operations** | **Compression Policies** (e.g. compress chunks older than 7 days), **Continuous Aggregates**-style rollups |

---

## The TSDB Edge

Why a **Time-Series Database** (TimescaleDB) was chosen over a standard **RDBMS** for this workload:

| Concern | TSDB (TimescaleDB) Approach | Typical RDBMS Limitation |
|---------|----------------------------|---------------------------|
| **High Write Throughput** | **Hypertables** split data into **time-based chunks**; inserts target only the latest chunk, reducing lock contention and improving **append** performance. | Single large table; more contention and index updates on every insert. |
| **Efficient Compression** | Native **columnar compression** with `segment_by` (e.g. `detector_id`) and `order_by` (e.g. `time DESC`) for time-ordered, **high compression ratios** on older chunks. | General-purpose row storage; less effective for time-ordered, repetitive metrics. |
| **Time-Centric Access** | **Chunk pruning**: time-range filters allow the engine to skip entire chunks, lowering **query latency**. **Indexing** on `(detector_id, time DESC)` aligns with common access patterns. | Indexes and storage not optimized for “latest by time” or “range by time + dimension.” |
| **Retention & Cost** | **Data retention policies** and **compression policies** (e.g. compress after 7 days) keep hot data fast and cold data small. | Manual partitioning and archiving; no built-in time-based automation. |

---

## SQL & Python Synergy

Clear separation of responsibilities: **Python** for ETL and orchestration, **SQL** for complex analytical queries.

### Python

- **ETL / Ingestion**: Parse **XML** (e.g. SUMO detector output), normalize **timestamps** (`datetime`, `timedelta`), and perform **batch inserts** (`executemany`) for high throughput.
- **Orchestration**: Drive connection lifecycle, run multi-step workflows (enable compression, add policies, run queries).
- **Post-processing**: **Linear interpolation** between two time points (e.g. estimate speed at a given second using neighboring rows); result formatting and reporting.
- **Extensibility**: Pipeline is structured so **Pandas** (DataFrames, resampling) or **NumPy** (array ops) can be added for heavier in-memory processing or pre-aggregation before load.

### SQL

- **Range queries**: Time-bounded filters (`time >= %s AND time <= %s`) with **aggregates** (`COUNT`, `AVG`, `MAX`, `MIN`) for a given detector and window.
- **Time-Series Aggregation**: **`time_bucket('5 minutes', time)`** for **downsampling**; `GROUP BY bucket, detector_id` with `AVG(flow)`, `MAX(flow)`, `SUM(nVehEntered)`.
- **Window Functions**: **`LAG(time) OVER (ORDER BY time)`** for **gap detection**; `EXTRACT(EPOCH FROM ...)` to compute gap duration in seconds.
- **Compression & Policies**: `ALTER TABLE ... SET (timescaledb.compress, ...)`, `add_compression_policy()`, `compress_chunk()`.

---

## Performance Optimization

| Technique | Implementation |
|-----------|----------------|
| **Cardinality & Indexing** | **Composite index** on `(detector_id, time DESC)` to support “by detector, by time” queries and avoid full scans. **Chunk_time_interval** of 1 day keeps chunk count and cardinality per chunk manageable. |
| **Query Latency** | **Chunk pruning** via time-range predicates; **index-only** access where possible. Range and aggregation queries limited to relevant chunks and index range. |
| **Write Throughput** | **Batch inserts** (e.g. 1000 rows per `executemany`) to reduce round-trips and transaction overhead. |
| **Storage & I/O** | **Compression** with `compress_segmentby = 'detector_id'` and `compress_orderby = 'time DESC'`; **compression policy** (e.g. compress chunks older than 7 days) to cut storage and improve read I/O on older data. |
| **Continuous Aggregates / Downsampling** | **On-the-fly** 5-minute **time_bucket** aggregation for dashboards or reporting; schema and pipeline support **materialized** continuous aggregates if needed for fixed windows. |

---

## Skills-to-Tool Mapping

| Skill / Concept | Tool or Feature Used in This Project |
|-----------------|--------------------------------------|
| **Time-Series Database** | **TimescaleDB** (PostgreSQL extension) |
| **Hypertable & Chunking** | `create_hypertable(..., chunk_time_interval => INTERVAL '1 day')` |
| **Time-Series Aggregation / Downsampling** | **`time_bucket('5 minutes', time)`** in SQL |
| **Window Functions** | **`LAG(time) OVER (ORDER BY time)`** for gap analysis |
| **Indexing for Time-Centric Queries** | **`CREATE INDEX ... ON detector_data (detector_id, time DESC)`** |
| **Data Retention / Compression** | **`add_compression_policy('detector_data', INTERVAL '7 days')`**, **`compress_chunk()`** |
| **Batch ETL** | **Python** `executemany()` with 1000-row batches |
| **Range Queries** | **SQL** `WHERE time >= ... AND time <= ...` with aggregates |
| **Gap Detection** | **SQL** CTE with **LAG** and **EXTRACT(EPOCH FROM ...)** |
| **Interpolation** | **Python** linear interpolation between two SQL-fetched points |
| **High Write Throughput** | **Hypertable** (chunking) + **batch insert** in Python |

---

## Repository Layout

| File | Purpose |
|------|---------|
| `setup_database_simple.py` | Creates DB, enables **TimescaleDB**, creates **hypertable**, **index**. |
| `ingest_data_simple.py` | **ETL**: reads SUMO detector XML, **batch loads** into `detector_data`. |
| `query_data_simple.py` | Enables **compression**, runs **range**, **interpolation**, **gap**, and **aggregation** queries. |
| `detector_output.xml` | Sample SUMO detector output (or similar) used for ingestion. |
| `simulation.sumocfg`, `simple.net.xml`, `simple.rou.xml` | SUMO simulation configuration and network/routes. |

---
🛠️ Tech Stack & SQL Synergy
Orchestration & ETL: Python (psycopg2) for batching (executemany) and timestamp normalization.

Database Engine: TimescaleDB (PostgreSQL Extension).

Analytical SQL: * time_bucket('5 minutes', time) for downsampling.

LAG(time) OVER (ORDER BY time) for gap analysis.

add_compression_policy() for automated storage optimization.

🚀 Getting Started
1. Prerequisites
PostgreSQL with TimescaleDB installed (e.g., via Docker or Timescale Forge).

Python 3.10+ and psycopg2.

2. Execution
Initialize DB: python setup_database_simple.py (Creates Hypertable & Indices).

Load Data: python ingest_data_simple.py (Performs batch ETL).

Run Analytics: python query_data_simple.py (Calculates aggregates & identifies gaps).

🛡️ License
MIT

👤 Developer
Irist – Specializing in High-Throughput Data Systems & Scalable Architecture.


