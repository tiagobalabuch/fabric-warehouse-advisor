# Performance Check Advisor

The Performance Check Advisor scans your Fabric Warehouse or SQL Analytics Endpoint for common performance pitfalls and produces actionable findings — no scoring, just clear guidance on what to fix and why.

## What it checks

| Category | What it detects |
|----------|----------------|
| **Warehouse Type** | Whether you're on a DataWarehouse or SQL Analytics Endpoint — gates subsequent checks |
| **Data Types** | `VARCHAR(MAX)`, oversized columns, `CHAR` vs `VARCHAR`, decimal over-precision, FLOAT for money, BIGINT for small ranges, dates stored as strings, nullable columns |
| **Caching** | Result set caching status, cache hit ratio, cold-start detection |
| **V-Order** | V-Order optimization state (DataWarehouse only — irreversible if disabled) |
| **Statistics** | Auto-create/update stats, proactive refresh, stale statistics, row count drift, tables without statistics |
| **Collation** | Detects columns whose collation differs from the database default — mismatched collation causes implicit conversions in joins and comparisons, preventing predicate push-down |
| **Query Regression** | Detects query shapes whose recent performance is significantly worse than their historical baseline (warehouse-wide) |
| **Custom SQL Pools** | Analyses Custom SQL Pools configuration — resource allocation imbalances, single-pool dominance, empty classifiers, pool pressure from Query Insights, unclassified traffic, and known Fabric app patterns not routed to a pool |

## Quick Start

```python
from fabric_warehouse_advisor import PerformanceCheckAdvisor, PerformanceCheckConfig

config = PerformanceCheckConfig(
    warehouse_name="MyWarehouse",
)

advisor = PerformanceCheckAdvisor(spark, config)
result = advisor.run()

# To experience all features and interactive capabilities, save the report and open it in a web browser
result.save("/lakehouse/default/Files/reports/report.html")
# Rich HTML report
displayHTML(result.html_report)
```

## Output Model

Unlike the Data Clustering advisor (which produces scores), the
Performance Check advisor produces **findings** at five severity levels:

| Level | Meaning |
|-------|---------|
| **CRITICAL** | Immediate action required — significant performance impact |
| **HIGH** | Important issue — should be addressed soon |
| **MEDIUM** | Worth reviewing — potential performance improvement |
| **LOW** | Minor concern — fix when convenient |
| **INFO** | Informational — current state is healthy or the item is for awareness |

Each finding includes:

- **Object name** — the specific table, column, or database affected
- **Message** — one-line summary of the issue
- **Detail** — context (current value, comparison, impact)
- **Recommendation** — actionable guidance
- **SQL fix** — ready-to-run T-SQL statement (when applicable)

## Working with Results

!!! tip "Web Browser is recommended"
    The best way to visualize the report is to save it as `HTML`, which provides the full experience with rich features and interactivity.

### Exploring Findings

```python
# Spark DataFrame with findings
display(result.findings)
```

### Saving Reports

```python
result.save("/lakehouse/default/Files/reports/performance_report.html")
result.save("/lakehouse/default/Files/reports/performance_report.md", "md")
result.save("/lakehouse/default/Files/reports/performance_report.txt", "txt")
```

### Persisting data to Delta table

```python
result.findings.write.mode("overwrite").format("delta").saveAsTable(
    "yourschema.performance_advisor"
)
```


### Filtering by Category or Level

```python
# Only critical findings
critical = [f for f in result.findings if f.is_critical]
display(critical)

# Only data type findings
from fabric_warehouse_advisor.advisors.performance_check.findings import CATEGORY_DATA_TYPES

dt_findings = result.summary.findings_by_category(CATEGORY_DATA_TYPES)
display(dt_findings)

# Other available categories:
# CATEGORY_WAREHOUSE_TYPE, CATEGORY_CACHING, CATEGORY_STATISTICS,
# CATEGORY_VORDER, CATEGORY_COLLATION, CATEGORY_QUERY_REGRESSION,
# CATEGORY_CUSTOM_SQL_POOLS
```

## Documentation

| Document | Description |
|----------|-------------|
| [How It Works](how-it-works.md) | The multi-phase pipeline |
| [Configuration](configuration.md) | Full parameter reference (~40 config fields) |
| [Checks Reference](checks.md) | Deep dive into each check category |
| [Reports](reports.md) | Text, Markdown, and HTML report formats |
