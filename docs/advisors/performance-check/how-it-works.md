# How It Works

The Performance Check advisor runs a **multi-phase pipeline (Phases 0–7)** that detects
the warehouse edition, then scans for caching misconfigurations, V-Order
issues, query regressions, data-type anti-patterns, statistics health
problems, collation mismatches, and Custom SQL Pools configuration.

Phases are split into two groups: **warehouse-level** checks (Phases 0–3)
run first without scope filtering, then table scope is resolved, and
**table-scoped** checks (Phases 4–6) run on the filtered set. Phase 7
(Custom SQL Pools) uses the Fabric REST API at the workspace level.

## Architecture Overview

```text

PerformanceCheckAdvisor.run()
  |
  Warehouse-level checks (no scope filtering):                      
  ├─ Phase 0: Edition Detection   → DATABASEPROPERTYEX()            
  ├─ Phase 1: Caching             → sys.databases, queryinsights    
  ├─ Phase 2: V-Order             → sys.databases                   
  ├─ Phase 3: Query Regression    → queryinsights.exec_requests     
  │                                                                 
  Scope resolution  (schema_names / table_names filtering)          
  │                                                                 
  Table-scoped checks:                                              
  ├─ Phase 4: Data Types          → INFORMATION_SCHEMA.COLUMNS      
  ├─ Phase 5: Statistics          → sys.stats, DBCC SHOW_STATS      
  ├─ Phase 6: Collation           → sys.columns, sys.databases      
  │                                                                 
  Workspace-level check (REST API + T-SQL):                         
  └─ Phase 7: Custom SQL Pools    → Fabric REST API, queryinsights  
  
```

!!! note
  All SQL runs via T-SQL passthrough (no data transferred to        
  Spark — only metadata and aggregates) 

## Phase 0: Warehouse Edition Detection

Detects whether the connected item is a **DataWarehouse** or a
**LakeWarehouse** (SQL Analytics Endpoint).

This is the **gating check** — it determines which subsequent phases
are applicable:

| Edition | Caching | V-Order | Query Regression | Data Types | Statistics | Collation | Custom SQL Pools |
|---------|---------|---------|------------------|-----------|-----------|-----------|------------------|
| DataWarehouse | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| LakeWarehouse | Yes | **Skipped** | Yes | Yes | Yes | Yes | Yes |

## Phase 1: Caching Analysis

Checks result-set caching configuration and analyses cold-start patterns.

### Sub-check 1a: Result Cache Status

- Caching **enabled** → INFO (healthy)
- Caching **disabled** → WARNING with `ALTER DATABASE` fix

### Sub-check 1b: Cold Start & Cache Hits

Queries `queryinsights.exec_requests_history` over a configurable
lookback window (`cold_start_lookback_hours`, default: 24h):

- Aggregates `result_cache_hit` values (0 = not cacheable, 1 = created, 2 = hit)
- Computes cache hit ratio → WARNING if below threshold
- Counts queries with `data_scanned_remote_storage_mb > 0` (cold starts)

## Phase 2: V-Order Check

Checks the V-Order write-time optimization state.

- **Auto-skipped** for LakeWarehouse editions
- V-Order **enabled** → INFO
- V-Order **disabled** → **CRITICAL** (irreversible setting)

!!! danger "V-Order is irreversible"
    Disabling V-Order cannot be undone. If V-Order is already disabled,
    the advisor recommends evaluating whether the warehouse is used for
    staging (acceptable) or analytics (problematic).

## Phase 3: Query Regression Detection

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

## Phase 4: Data Type Analysis

Reads `INFORMATION_SCHEMA.COLUMNS` joined with `INFORMATION_SCHEMA.TABLES`
to scan every column in user tables for data-type anti-patterns.

This phase uses a single T-SQL query that returns all column metadata,
then applies 9 configurable heuristic checks locally:

1. `VARCHAR(MAX)` → **CRITICAL**
2. Oversized `VARCHAR(n)` → WARNING
3. `CHAR(n)` where `VARCHAR(n)` is better → WARNING
4. `DECIMAL` / `NUMERIC` with excessive precision → WARNING
5. `FLOAT` / `REAL` on monetary-sounding columns → WARNING
6. `BIGINT` for small-range values → INFO
7. Date/time data stored as strings → WARNING
8. Nullable columns that should be `NOT NULL` → INFO

Column names are matched against regex patterns to detect semantic
mismatches (e.g., a `FLOAT` column named `total_amount`).

Scope filtering (`schema_names`, `table_names`, `min_row_count`) is
applied before analysis.

## Phase 5: Statistics Health

The most comprehensive phase, with four sub-checks:

### Sub-check 5a: Database Configuration

- Auto-create or auto-update disabled → **CRITICAL**

### Sub-check 5b: Proactive Statistics Refresh

- Proactive refresh disabled → WARNING

### Sub-check 5c: Statistics Staleness & Row Drift

Queries `sys.stats` joined with `sys.objects`, `sys.stats_columns`, and
`sys.columns` for all user tables. For each statistics object:

- **Staleness**: compares `STATS_DATE()` against the configurable threshold (`stale_stats_threshold_days`, default: 7 days)
- **Row drift**: uses `DBCC SHOW_STATISTICS ... WITH STAT_HEADER` to compare the statistics row count estimate against the actual row count obtained via `COUNT_BIG(*)` (which Fabric resolves from columnstore metadata). The earlier approach using `sys.partitions` was replaced because it returned `NULL` on Fabric Warehouse. Drift above `row_drift_pct_threshold` (default: 20%) triggers a finding.

### Sub-check 5d: Tables Without Statistics

Identifies user tables that have no statistics objects at all —
typically new tables that haven't been queried yet.

## Phase 6: Collation Consistency

Checks column-level collation against the database default collation. Mismatched collation can cause implicit conversions in joins and comparisons, preventing predicate push-down.

- All columns match database collation → INFO
- Column collation differs → WARNING

## Phase 7: Custom SQL Pools

Analyses the Custom SQL Pools configuration for the workspace and monitors pool pressure.

### Configuration checks

- **Feature enabled/disabled** — awareness of whether the workspace
  uses Custom SQL Pools or default autonomous workload management
- **Resource allocation sum ≠ 100%** — misconfigured pools
- **Low resource allocation per pool** — risk on capacity SKU downscale
- **Single pool dominance** — ≥ 90% allocated to one pool
- **Empty classifier values** — pools with no routing rules
- **Read optimization heuristic** — suggests enabling for reporting pools
  (detected by name or classifier patterns)
- **Pool count near limit** — approaching the 8-pool maximum

### Runtime checks

- **Pool under pressure** — queries `queryinsights.sql_pool_insights`
  for `is_pool_under_pressure = 1` events over a configurable lookback
  window
- **Unclassified traffic** — compares `program_name` values from
  `queryinsights.exec_requests_history` against pool classifiers to
  find traffic not routed to any pool
- **Known Fabric app patterns** — detects well-known Fabric application
  names (Pipelines, Power BI, SQL Query Editor) that are not matched
  by any classifier

## Performance Characteristics

| Phase | Method | Data Transfer | Speed |
|-------|--------|---------------|-------|
| 0. Edition | T-SQL passthrough | ~1 row | Instant |
| 1. Caching | T-SQL passthrough | Aggregated stats (~KB) | Fast |
| 2. V-Order | T-SQL passthrough | ~1 row | Instant |
| 3. Query Regression | T-SQL passthrough | Aggregated medians (~KB) | Fast |
| 4. Data Types | T-SQL passthrough | Column metadata (~KB) | Fast |
| 5. Statistics | T-SQL passthrough + DBCC | Metadata + stat headers | Fast |
| 6. Collation | T-SQL passthrough | Column metadata (~KB) | Fast |
| 7. Custom SQL Pools | REST API + T-SQL passthrough | Config JSON + aggregates (~KB) | Fast |

!!! info 
    No user data is ever transferred to Spark — only metadata, counts, and aggregates.
