# Check Categories

The Performance Check advisor runs up to **7 check categories**, each
targeting a different area of warehouse health.  Every finding includes
a severity level, a human-readable message, and — where applicable — a
ready-to-run T-SQL fix.

---

## 1. Warehouse Type Detection

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_WAREHOUSE_TYPE` |
| Config toggle | Always runs |
| Applies to | DataWarehouse, LakeWarehouse |

Detects the Fabric item edition via:

```sql
SELECT CONVERT(varchar(100),
       DATABASEPROPERTYEX(DB_NAME(), 'Edition')) AS edition
```

The result gates subsequent checks — for example, V-Order is only
meaningful on **DataWarehouse**.

| Check | Level | When |
|-------|-------|------|
| `edition_detected` | INFO | Edition successfully determined |
| `edition_detection_failed` | INFO | Query failed (defaults to Unknown) |

---

## 2. Data Types

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_DATA_TYPES` |
| Config toggle | `check_data_types` |
| Applies to | DataWarehouse, LakeWarehouse |

Scans `INFORMATION_SCHEMA.COLUMNS` for column-level anti-patterns
that hurt performance in Fabric's columnar Delta Parquet engine.

### Full Check List

| Check Name | Level | What It Detects | Why It Matters |
|-----------|-------|-----------------|----------------|
| `varchar_max_detected` | CRITICAL | `VARCHAR(MAX)` | Engine allocates maximum memory during sort/hash, causing spills. Disqualifies queries from result set caching. |
| `oversized_varchar` | WARNING | `VARCHAR(n)` at or above `oversized_varchar_threshold` (default 8 000) | Inflated cost estimates; statistics less accurate when declared length far exceeds actual data. |
| `char_used_instead_of_varchar` | WARNING | `CHAR(n)` columns | Fixed-length padding wastes space when actual values are shorter than `n`. |
| `decimal_over_precision` | WARNING | `DECIMAL` / `NUMERIC` with precision above `decimal_over_precision_threshold` (default 18) | Over-provisioned precision increases per-row storage cost. |
| `float_for_monetary_data` | WARNING | `FLOAT` / `REAL` on columns whose name matches a monetary pattern | Approximate types introduce rounding errors in financial calculations. |
| `bigint_for_small_range` | INFO | `BIGINT` on columns whose name suggests small-range values (year, month, qty, …) | `INT` (4 bytes) or `SMALLINT` (2 bytes) would be sufficient. |
| `datetime_stored_as_string` | WARNING | String columns whose name suggests date/time data | Prevents date arithmetic, disables predicate pushdown, increases storage. |
| `nullable_column` | INFO | Nullable columns with names suggesting required fields (id, key, code, …) | Null bitmaps add metadata overhead; NOT NULL improves statistics and optimizations. |

### Name-Pattern Heuristics

The data type checks use regex patterns to infer column purpose from
its name:

| Pattern | Matches (examples) | Used By |
|---------|---------------------|---------|
| Date/time | `created_at`, `order_date`, `start_dt`, `timestamp` | `datetime_stored_as_string` |
| Monetary | `amount`, `price`, `total_cost`, `net_revenue`, `discount` | `float_for_monetary_data` |
| Small-range | `year`, `month`, `qty`, `status`, `version`, `rank` | `bigint_for_small_range` |
| Required field | `*_id`, `*_key`, `*_code`, `pk_*`, `sk_*`, `fk_*` | `nullable_column` |

### Configuration Knobs

Every individual data type check can be toggled independently.  See the
[Configuration Reference](configuration.md#data-type-thresholds) for
all available parameters.

### Example Findings

```
❌ [dbo].[FactSales].[Description]
   VARCHAR(MAX) column detected.
   The engine allocates maximum potential memory during sort/hash
   operations, causing memory spills and slow queries.
   → Determine the actual max length with:
     SELECT MAX(LEN([Description])) FROM [dbo].[FactSales]

⚠️ [dbo].[DimCustomer].[FullName]
   VARCHAR(4000) is excessively large.
   → Check actual max length and resize.
```

---

## 3. Caching

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_CACHING` |
| Config toggle | `check_caching` |
| Applies to | DataWarehouse |

Analyses result-set caching configuration and cold-start behaviour
using `sys.databases` and `queryinsights.exec_requests_history`.

### Full Check List

| Check Name | Level | What It Detects |
|-----------|-------|-----------------|
| `result_cache_enabled` | INFO | Result set caching is ON (good) |
| `result_cache_disabled` | WARNING | Result set caching is OFF |
| `result_cache_status_unknown` | WARNING | Could not determine status |
| `cache_hit_summary` | INFO | Summary of cache hits/misses over the lookback window |
| `low_cache_hit_ratio` | WARNING | Hit ratio below `cache_hit_ratio_warning_threshold` (default 30%) |
| `cold_start_detected` | INFO / WARNING | Queries fetching data from remote storage (OneLake) |
| `no_query_history` | INFO | No records in Query Insights for the lookback period |

### How Caching Analysis Works

**Step 1 — Result Cache Status**

```sql
SELECT name, is_result_set_caching_on
FROM sys.databases
WHERE database_id = DB_ID()
```

**Step 2 — Cache Hit Ratio** (over `cold_start_lookback_hours`)

Aggregates `queryinsights.exec_requests_history` by `result_cache_hit`:

| Value | Meaning |
|-------|---------|
| `0` | Not cacheable |
| `1` | Cache create (first execution, result stored) |
| `2` | Cache hit (reused previous result) |

**Step 3 — Cold Start Detection**

Queries with `data_scanned_remote_storage_mb > 0` fetched data from
OneLake rather than local SSD cache.  This is normal for first-run
queries but should decrease on subsequent runs.

### SQL Fix

If result set caching is disabled:

```sql
ALTER DATABASE [MyWarehouse]
SET RESULT_SET_CACHING ON;
```

---

## 4. V-Order Optimization

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_VORDER` |
| Config toggle | `check_vorder` |
| Applies to | **DataWarehouse only** |

Checks the V-Order write-time optimization state.  V-Order applies
special sorting, row group distribution, dictionary encoding, and
compression to Parquet files.

### Full Check List

| Check Name | Level | What It Detects |
|-----------|-------|-----------------|
| `vorder_enabled` | INFO | V-Order is ON (recommended) |
| `vorder_disabled` | CRITICAL | V-Order is OFF — **irreversible** |
| `vorder_not_applicable` | INFO | Connected to a LakeWarehouse; check skipped |
| `vorder_status_unknown` | WARNING | Could not determine status |

!!! danger "Irreversible Setting"
    Disabling V-Order **cannot be undone**.  Once disabled, new Parquet
    files lose the V-Order optimizations.  This is flagged as CRITICAL
    because:

    - Power BI Direct Lake mode depends on V-Order.
    - Read performance can degrade by **10–50%**.
    - The only recovery path is to create a new warehouse and reload data.

### When V-Order OFF Is Acceptable

A staging warehouse used purely for ETL ingestion (no reporting) can
reasonably have V-Order disabled.  The common pattern:

1. **Staging warehouse** — V-Order OFF, high-throughput ingestion.
2. **Reporting warehouse** — V-Order ON, processed data loaded via
   `INSERT INTO ... SELECT` or Fabric pipelines.

---

## 5. Statistics Health

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_STATISTICS` |
| Config toggle | `check_statistics` |
| Applies to | DataWarehouse, LakeWarehouse |

Analyses the health of query optimizer statistics using `sys.stats`, `STATS_DATE()`, and `DBCC SHOW_STATISTICS`.

### Full Check List

| Check Name | Level | What It Detects |
|-----------|-------|-----------------|
| `auto_create_stats_on` | INFO | Auto-create statistics is enabled |
| `auto_create_stats_off` | CRITICAL | Auto-create statistics is disabled |
| `auto_update_stats_on` | INFO | Auto-update statistics is enabled |
| `auto_update_stats_off` | CRITICAL | Auto-update statistics is disabled |
| `proactive_refresh_on` | INFO | Proactive refresh is enabled |
| `proactive_refresh_off` | WARNING | Proactive refresh is disabled |
| `stale_statistics` | WARNING | Statistics older than `stale_stats_threshold_days` (default 7) |
| `row_count_drift` | WARNING / CRITICAL | Actual rows differ from stats estimate by more than `row_drift_pct_threshold` (default 20%). CRITICAL if drift > 50%. |
| `no_statistics` | WARNING | Table has rows but no statistics objects |

### How Statistics Analysis Works

**Database-level settings** are checked first:

```sql
SELECT name, is_auto_create_stats_on, is_auto_update_stats_on
FROM sys.databases WHERE database_id = DB_ID()
```

**Proactive refresh** (Fabric-specific feature):

```sql
SELECT name, is_proactive_stats_collection_on
FROM sys.databases WHERE database_id = DB_ID()
```

**Per-table staleness** — iterates all statistics from `sys.stats` and
checks `STATS_DATE()` against the configured threshold.

**Row count drift** — compares the `COUNT_BIG(*)` result, which Fabric resolves from columnstore metadata, to the estimate from `DBCC SHOW_STATISTICS ... WITH STAT_HEADER`. A large drift means the optimizer is working with outdated cardinality estimates.

### SQL Fixes

Update stale statistics:

```sql
UPDATE STATISTICS [schema].[table] (stats_name) WITH FULLSCAN;
```

Enable proactive refresh:

```sql
ALTER DATABASE CURRENT
SET PROACTIVE_STATS_COLLECTION = ON;
```

---

## 6. Query Regression

| Property | Value |
|----------|-------|
| Category constant | `CATEGORY_QUERY_REGRESSION` |
| Config toggle | `check_query_regression` |
| Applies to | DataWarehouse, LakeWarehouse |

Detects query shapes whose recent performance has significantly
regressed compared to a historical baseline. Both windows are
computed from `queryinsights.exec_requests_history`, ensuring
no overlap between baseline and recent periods.

!!! note "Warehouse-wide"
    This check runs warehouse-wide and is **not** filtered by
    `schema_names` / `table_names` selections.

### Full Check List

| Check Name | Level | What It Detects |
|-----------|-------|------------------|
| `query_regression_detected` | WARNING | Query median elapsed time ≥ 2× baseline median (`regression_factor_warning`) |
| `query_regression_detected` | CRITICAL | Query median elapsed time ≥ 5× baseline median (`regression_factor_critical`) |
| `no_regression_detected` | INFO | No regressions found within configured thresholds |
| `regression_check_error` | INFO | Check could not be executed (query error) |

### How It Works

The 30-day Query Insights retention window is split into:

- **Baseline**: `DATEADD(day, -30, GETUTCDATE())` to `DATEADD(day, -N, GETUTCDATE())`
- **Recent**: last *N* days (configured by `regression_lookback_days`, default: 7)

For each `query_hash`, the median `total_elapsed_time_ms` is computed
using `PERCENTILE_CONT(0.5)`. Both windows require at least
`regression_min_executions` (default: 3) runs.

The **regression factor** = recent_median / baseline_median. Queries
exceeding the warning threshold are flagged.

### Configuration

| Parameter | Default | Effect |
|-----------|---------|--------|
| `check_query_regression` | `True` | Enable/disable the check |
| `regression_lookback_days` | `7` | Number of days in the "recent" window |
| `regression_factor_warning` | `2.0` | Trigger WARNING at this multiplier |
| `regression_factor_critical` | `5.0` | Trigger CRITICAL at this multiplier |
| `regression_min_executions` | `3` | Minimum executions in each window |

---

## Severity Reference

| Level | Icon | Meaning | Action |
|-------|------|---------|--------|
| CRITICAL | ❌ | Significant performance impact or irreversible misconfiguration | Fix immediately |
| HIGH | 🔴 | Important issue likely to affect performance at scale | Plan a fix soon |
| MEDIUM | 🟡 | Sub-optimal setting that may degrade performance under load | Fix during next maintenance window |
| LOW | 🔵 | Minor improvement opportunity with marginal benefit | Address when convenient |
| INFO | ✅ | Healthy configuration or informational note | No action required |
