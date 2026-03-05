# Configuration Reference

All parameters are fields of the `DataClusteringAdvisorConfig` dataclass.
Create an instance, override the defaults you need, and pass it to
`DataClusteringAdvisor`.

```python
from fabric_data_clustering_advisor import DataClusteringAdvisor, DataClusteringAdvisorConfig

config = DataClusteringAdvisorConfig(
    warehouse_name="MyWarehouse",
    min_row_count=500_000,
    verbose=True,
)

advisor = DataClusteringAdvisor(spark, config)
result = advisor.run()
```

## Connection Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `warehouse_name` | `str` | `""` | **Required.** The Fabric Warehouse name used in the Spark connector's three-part naming. |
| `workspace_id` | `str` | `""` | Workspace GUID. Only needed for [cross-workspace](cross-workspace.md) access. |
| `warehouse_id` | `str` | `""` | Warehouse item GUID. Only needed for [cross-workspace](cross-workspace.md) access. |

## Threshold Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_row_count` | `int` | `1_000_000` | Minimum rows for a table to be analysed. Tables below this are skipped entirely. |
| `large_table_rows` | `int` | `50_000_000` | Row-count threshold for the maximum table-size score. Tables at 10× this value get full points. |
| `min_predicate_hits` | `int` | `2` | Minimum number of times a column must appear in WHERE predicates to be considered a candidate. |
| `min_query_runs` | `int` | `2` | Minimum number of executions for a query to be treated as "frequently run" in Query Insights. |

## Cardinality Classification

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `low_cardinality_upper` | `float` | `0.001` | Cardinality ratio below which a column is classified as **Low**. |
| `high_cardinality_lower` | `float` | `0.05` | Cardinality ratio at or above which a column is classified as **High**. |
| `low_cardinality_abs_max` | `int` | `50` | Absolute distinct-count ceiling — any column with ≤ this many distinct values is always classified as **Low**, regardless of the ratio. |
| `cardinality_sample_fraction` | `float` | `1.0` | Fraction of the table to sample when the Spark fallback path is used. Ignored when T-SQL passthrough succeeds (which is the default). Must be in `(0, 1.0]`. |

### How classification works

```
if approx_distinct <= low_cardinality_abs_max:
    → "Low"
elif ratio < low_cardinality_upper:
    → "Low"
elif ratio >= high_cardinality_lower:
    → "High"
else:
    → "Medium"
```

Where `ratio = approx_distinct / total_rows`.

## Scoring Weights

The composite score is the sum of four weighted factors. The weights
**must sum to 100** — the config validates this at runtime and raises
`ValueError` if they don't.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `score_weight_table_size` | `int` | `30` | Maximum points for the table-size factor. |
| `score_weight_predicate_freq` | `int` | `30` | Maximum points for predicate frequency. |
| `score_weight_cardinality` | `int` | `25` | Maximum points for column cardinality. |
| `score_weight_data_type` | `int` | `15` | Maximum points for data-type support. |

See [Scoring](scoring.md) for the detailed formulas.

## Recommendation Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_clustering_columns` | `int` | `3` | Warn when a table already has more clustered columns than this. Does **not** limit CTAS output. |
| `min_recommendation_score` | `int` | `40` | Minimum composite score to surface a recommendation. Columns below this are labelled "Not recommended". |
| `generate_ctas` | `bool` | `False` | When `True`, generate one `CREATE TABLE ... AS SELECT` DDL statement per recommended column. Set this to include ready-to-run DDL in the report. |

## Filtering

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table_names` | `list[str]` | `[]` | Restrict analysis to specific tables. Each entry can be `"table_name"` (matches any schema) or `"schema.table_name"` (exact match). Empty list = all tables. |

Examples:

```python
# Analyse only these two tables
config = DataClusteringAdvisorConfig(
    warehouse_name="MyWarehouse",
    table_names=["dbo.Orders", "FactSales"],
)

# Analyse only tables in the 'sales' schema (by listing them explicitly)
config = DataClusteringAdvisorConfig(
    warehouse_name="MyWarehouse",
    table_names=["sales.FactOrders", "sales.FactReturns", "sales.FactLineItems"],
)
```

The filter applies to **all phases** — metadata collection, row counting,
query pattern matching, cardinality estimation, and scoring.

## Output Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `verbose` | `bool` | `False` | When `True`, prints structured debug output for each phase including intermediate DataFrames, row counts, and predicate breakdowns. Useful for understanding what the advisor is doing. |

## Validation

The config is validated automatically when `advisor.run()` is called. The
following checks are performed:

- `warehouse_name` must be set to a non-empty value (not the placeholder
  `"<your_warehouse_name>"`)
- `cardinality_sample_fraction` must be in `(0, 1.0]`
- Score weights must sum to exactly 100

If any check fails, a `ValueError` is raised with a descriptive message.
