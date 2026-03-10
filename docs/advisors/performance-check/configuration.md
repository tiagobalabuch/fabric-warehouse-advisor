# Configuration Reference

All parameters are fields of the `PerformanceCheckConfig` dataclass.
Create an instance, override the defaults you need, and pass it to
`PerformanceCheckAdvisor`.

```python
from fabric_warehouse_advisor import PerformanceCheckAdvisor, PerformanceCheckConfig

config = PerformanceCheckConfig(
    warehouse_name="MyWarehouse",
    check_vorder=True,
    stale_stats_threshold_days=14,
    verbose=True,
)

advisor = PerformanceCheckAdvisor(spark, config)
result = advisor.run()
```

## Connection Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `warehouse_name` | `str` | `""` | **Required.** The Fabric Warehouse or Lakehouse SQL Endpoint name. |
| `workspace_id` | `str` | `""` | Workspace GUID. Only needed for [cross-workspace](../../cross-workspace.md) access. |
| `warehouse_id` | `str` | `""` | Warehouse item GUID. Only needed for [cross-workspace](../../cross-workspace.md) access. |
| `sql_endpoint_id` | `str` | `""` | SQL Endpoint item GUID. For [cross-workspace](../../cross-workspace.md) access to a Lakehouse SQL Analytics Endpoint. |

## Scope Filtering

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `schema_names` | `list[str]` | `[]` | Restrict analysis to specific schemas. Empty = all user schemas. |
| `table_names` | `list[str]` | `[]` | Restrict analysis to specific tables. Each entry can be `"table_name"` (any schema) or `"schema.table_name"`. Empty = all tables. |

Examples:

```python
# Only check tables in the 'sales' schema
config = PerformanceCheckConfig(
    warehouse_name="MyWarehouse",
    schema_names=["sales"],
)

# Only check specific tables
config = PerformanceCheckConfig(
    warehouse_name="MyWarehouse",
    table_names=["dbo.FactSales", "dbo.DimCustomer"],
)
```

## Check Category Toggles

Each check category can be independently enabled or disabled:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `check_data_types` | `bool` | `True` | Enable data-type anti-pattern analysis. |
| `check_caching` | `bool` | `True` | Enable caching configuration analysis. |
| `check_statistics` | `bool` | `True` | Enable statistics health analysis. |
| `check_vorder` | `bool` | `True` | Enable V-Order state check. Auto-skipped for LakeWarehouse. |
| `check_collation` | `bool` | `True` | Enable collation mismatch check. |
| `check_query_regression` | `bool` | `True` | Enable query regression detection. |

Example — run only the statistics check:

```python
config = PerformanceCheckConfig(
    warehouse_name="MyWarehouse",
    check_data_types=False,
    check_caching=False,
    check_vorder=False,
    check_statistics=True,
    check_collation=False,
    check_query_regression=False,
)
```

## Data Type Thresholds

Fine-grained control over which data-type patterns are flagged:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `varchar_max_warning` | `bool` | `True` | Flag `VARCHAR(MAX)` and `NVARCHAR(MAX)` columns. |
| `nvarchar_to_varchar_warning` | `bool` | `True` | Flag `NVARCHAR` columns that could potentially be `VARCHAR`. |
| `char_to_varchar_warning` | `bool` | `True` | Flag `CHAR(n)` columns where `VARCHAR(n)` may be better. |
| `nullable_warning` | `bool` | `True` | Flag nullable columns that could be `NOT NULL`. |
| `oversized_varchar_threshold` | `int` | `8000` | Flag `VARCHAR(n)` / `NVARCHAR(n)` at or above this length. |
| `decimal_over_precision_threshold` | `int` | `18` | Flag `DECIMAL` / `NUMERIC` with precision above this value. |
| `datetime_as_string_warning` | `bool` | `True` | Flag string columns whose names suggest date/time data. |
| `float_for_money_warning` | `bool` | `True` | Flag `FLOAT` / `REAL` on monetary-sounding columns. |
| `bigint_where_int_suffices_warning` | `bool` | `True` | Flag `BIGINT` on columns suggesting small-range values. |

Example — only flag critical issues:

```python
config = PerformanceCheckConfig(
    warehouse_name="MyWarehouse",
    varchar_max_warning=True,       # CRITICAL: keep on
    char_to_varchar_warning=False,  # suppress CHAR warnings
    nullable_warning=False,         # suppress nullable INFO
    nvarchar_to_varchar_warning=False,
)
```

## Caching Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `result_cache_check` | `bool` | `True` | Check whether result set caching is enabled. |
| `cold_start_analysis` | `bool` | `True` | Analyse cold-start patterns from Query Insights. |
| `cold_start_lookback_hours` | `int` | `24` | How many hours back to look in `exec_requests_history`. |
| `cache_hit_ratio_warning_threshold` | `float` | `0.3` | Warn if the cache hit ratio is below this fraction (30%). |

## Statistics Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stale_stats_threshold_days` | `int` | `7` | Warn if statistics are older than this many days. |
| `row_drift_pct_threshold` | `float` | `20.0` | Warn if actual row count differs from stats estimate by more than this %. |
| `proactive_refresh_check` | `bool` | `True` | Check if proactive statistics refresh is enabled. |
| `orphaned_stats_check` | `bool` | `True` | Check for orphaned statistics objects. |
| `tables_without_stats_check` | `bool` | `True` | Check for tables that have no statistics at all. |

## Query Regression Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `regression_lookback_days` | `int` | `7` | Window (days) defining the "recent" period. Baseline is the preceding historical range within Query Insights' 30-day retention. |
| `regression_factor_warning` | `float` | `2.0` | Flag a query as WARNING when its recent median duration is at least this many times the baseline median. |
| `regression_factor_critical` | `float` | `5.0` | Flag a query as CRITICAL when its recent median duration is at least this many times the baseline median. |
| `regression_min_executions` | `int` | `3` | Minimum executions required in **both** baseline and recent windows before a query is compared. |

## V-Order Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `vorder_warn_if_disabled` | `bool` | `True` | Flag if V-Order is disabled on the warehouse. |

## Output

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `verbose` | `bool` | `False` | Print intermediate debug output for each phase. |

## Validation

The config is validated automatically when `advisor.run()` is called:

- `warehouse_name` must be set to a non-empty value (not the placeholder
  `"<your_warehouse_name>"`)

If the check fails, a `ValueError` is raised with a descriptive message.
