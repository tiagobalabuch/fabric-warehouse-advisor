# How It Works

The Performance Check advisor runs a **multi-phase pipeline (Phases 0–6)** that detects
the warehouse edition, then scans for data-type anti-patterns, caching
misconfigurations, V-Order issues, statistics health problems, collation
mismatches, and query regressions.

## Architecture Overview

```text
┌────────────────────────────────────────────────────────────────┐
│                     Fabric Notebook                            │
│                                                                │
│  │                                                             │
│  ├─ Phase 0: Edition Detection   → DATABASEPROPERTYEX()        │
│  ├─ Phase 1: Data Types          → INFORMATION_SCHEMA.COLUMNS  │
│  ├─ Phase 2: Caching             → sys.databases, queryinsights│
│  ├─ Phase 3: V-Order             → sys.databases               │
│  ├─ Phase 4: Statistics          → sys.stats, DBCC SHOW_STATS  │
│  ├─ Phase 5: Collation           → sys.columns, sys.databases  │
│  └─ Phase 6: Query Regression    → queryinsights.exec_requests │
│                                                                │
│  All SQL runs via T-SQL passthrough (no data transferred to    │
│  Spark — only metadata and aggregates)                         │
└────────────────────────────────────────────────────────────────┘
```

## Phase 0: Warehouse Edition Detection

Detects whether the connected item is a **DataWarehouse** or a
**LakeWarehouse** (SQL Analytics Endpoint).

```sql
SELECT CONVERT(varchar(100), DATABASEPROPERTYEX(DB_NAME(), 'Edition')) AS edition
```

This is the **gating check** — it determines which subsequent phases
are applicable:

| Edition | Data Types | Caching | V-Order | Statistics | Collation | Query Regression |
|---------|-----------|---------|---------|-----------|-----------|------------------|
| DataWarehouse | Yes | Yes | Yes | Yes | Yes | Yes |
| LakeWarehouse | Yes | Yes | **Skipped** | Yes | Yes | Yes |

## Phase 1: Data Type Analysis

Reads `INFORMATION_SCHEMA.COLUMNS` joined with `INFORMATION_SCHEMA.TABLES`
to scan every column in user tables for data-type anti-patterns.

This phase uses a single T-SQL query that returns all column metadata,
then applies 9 configurable heuristic checks locally:

1. `VARCHAR(MAX)` / `NVARCHAR(MAX)` → **CRITICAL**
2. Oversized `VARCHAR(n)` / `NVARCHAR(n)` → WARNING
3. `CHAR(n)` where `VARCHAR(n)` is better → WARNING
4. `NVARCHAR` where `VARCHAR` might suffice → INFO
5. `DECIMAL` / `NUMERIC` with excessive precision → WARNING
6. `FLOAT` / `REAL` on monetary-sounding columns → WARNING
7. `BIGINT` for small-range values → INFO
8. Date/time data stored as strings → WARNING
9. Nullable columns that should be `NOT NULL` → INFO

Column names are matched against regex patterns to detect semantic
mismatches (e.g., a `FLOAT` column named `total_amount`).

Scope filtering (`schema_names`, `table_names`, `min_row_count`) is
applied before analysis.

## Phase 2: Caching Analysis

Checks result-set caching configuration and analyses cold-start patterns.

### Sub-check 2a: Result Cache Status

```sql
SELECT name, is_result_set_caching_on
FROM sys.databases WHERE database_id = DB_ID()
```

- Caching **enabled** → INFO (healthy)
- Caching **disabled** → WARNING with `ALTER DATABASE` fix

### Sub-check 2b: Cold Start & Cache Hits

Queries `queryinsights.exec_requests_history` over a configurable
lookback window (`cold_start_lookback_hours`, default: 24h):

- Aggregates `result_cache_hit` values (0 = not cacheable, 1 = created, 2 = hit)
- Computes cache hit ratio → WARNING if below threshold
- Counts queries with `data_scanned_remote_storage_mb > 0` (cold starts)

## Phase 3: V-Order Check

Checks the V-Order write-time optimization state.

```sql
SELECT [name], [is_vorder_enabled] FROM sys.databases
```

- **Auto-skipped** for LakeWarehouse editions
- V-Order **enabled** → INFO
- V-Order **disabled** → **CRITICAL** (irreversible setting)

!!! warning "V-Order is irreversible"
    Disabling V-Order cannot be undone. If V-Order is already disabled,
    the advisor recommends evaluating whether the warehouse is used for
    staging (acceptable) or analytics (problematic).

## Phase 4: Statistics Health

The most comprehensive phase, with four sub-checks:

### Sub-check 4a: Database Configuration

```sql
SELECT name, is_auto_create_stats_on, is_auto_update_stats_on
FROM sys.databases WHERE database_id = DB_ID()
```

- Auto-create or auto-update disabled → **CRITICAL**

### Sub-check 4b: Proactive Statistics Refresh

```sql
SELECT name, is_proactive_stats_collection_on
FROM sys.databases WHERE database_id = DB_ID()
```

- Proactive refresh disabled → WARNING

### Sub-check 4c: Statistics Staleness & Row Drift

Queries `sys.stats` joined with `sys.objects`, `sys.stats_columns`, and
`sys.columns` for all user tables. For each statistics object:

- **Staleness**: compares `STATS_DATE()` against the configurable
  threshold (`stale_stats_threshold_days`, default: 7 days)
- **Row drift**: uses `DBCC SHOW_STATISTICS ... WITH STAT_HEADER` to
  compare the statistics row count estimate against the actual row count
  obtained via `COUNT_BIG(*)` (which Fabric resolves from columnstore
  metadata). The earlier approach using `sys.partitions` was replaced
  because it returned `NULL` on Fabric Warehouse. Drift above
  `row_drift_pct_threshold` (default: 20%) triggers a finding.

### Sub-check 4d: Tables Without Statistics

Identifies user tables that have no statistics objects at all —
typically new tables that haven't been queried yet.

## Phase 5: Collation Consistency

Checks column-level collation against the database default collation.
Mismatched collation can cause implicit conversions in joins and
comparisons, preventing predicate push-down.

```sql
SELECT collation_name FROM sys.databases WHERE database_id = DB_ID()
```

- All columns match database collation → INFO
- Column collation differs → WARNING

## Phase 6: Query Regression Detection

Compares recent query performance against a historical baseline
using `queryinsights.exec_requests_history`.

!!! note "Warehouse-wide"
    This check runs warehouse-wide and is **not** filtered by
    `schema_names` / `table_names` selections.

The 30-day Query Insights retention window is split into two periods:

- **Baseline**: days 8–30 (configurable via `regression_lookback_days`)
- **Recent**: last 7 days (default)

Query shapes are identified by `query_hash`. Both windows require a
minimum number of executions (`regression_min_executions`, default: 3)
to filter noise.

| Regression factor | Level |
|---|---|
| ≥ 5× baseline | CRITICAL |
| ≥ 2× baseline | WARNING |

## Data Flow

```text
Phase 0 (edition) ──────────────── gates Phase 3
Phase 1 (data types) ──────────┐
Phase 2 (caching) ─────────────┤
Phase 3 (V-Order) ─────────────┤
Phase 4 (statistics) ──────────┼──► Report Generation
Phase 5 (collation) ───────────┤        │
Phase 6 (query regression) ────┘        ▼
                               PerformanceCheckResult
                               ├── findings[]
                               ├── summary (CheckSummary)
                               ├── text_report
                               ├── markdown_report
                               └── html_report
```

## Performance Characteristics

| Phase | Method | Data Transfer | Speed |
|-------|--------|---------------|-------|
| 0. Edition | T-SQL passthrough | ~1 row | Instant |
| 1. Data Types | T-SQL passthrough | Column metadata (~KB) | Fast |
| 2. Caching | T-SQL passthrough | Aggregated stats (~KB) | Fast |
| 3. V-Order | T-SQL passthrough | ~1 row | Instant |
| 4. Statistics | T-SQL passthrough + DBCC | Metadata + stat headers | Fast |
| 5. Collation | T-SQL passthrough | Column metadata (~KB) | Fast |
| 6. Query Regression | T-SQL passthrough | Aggregated medians (~KB) | Fast |

No user data is ever transferred to Spark — only metadata, counts,
and aggregates.
