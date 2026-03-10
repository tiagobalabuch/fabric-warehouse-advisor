# Performance Check Advisor

The Performance Check Advisor scans your Fabric Warehouse (or Lakehouse
SQL Endpoint) for common performance pitfalls and produces actionable
findings — no scoring, just clear guidance on what to fix and why.

## What it checks

| Category | What it detects |
|----------|----------------|
| **Warehouse Type** | Whether you're on a DataWarehouse or Lakehouse SQL Endpoint — gates subsequent checks |
| **Data Types** | `VARCHAR(MAX)`, oversized columns, `CHAR` vs `VARCHAR`, `NVARCHAR` review, decimal over-precision, FLOAT for money, BIGINT for small ranges, dates stored as strings, nullable columns |
| **Caching** | Result set caching status, cache hit ratio, cold-start detection |
| **V-Order** | V-Order optimization state (DataWarehouse only — irreversible if disabled) |
| **Statistics** | Auto-create/update stats, proactive refresh, stale statistics, row count drift, tables without statistics |
| **Query Regression** | Detects query shapes whose recent performance is significantly worse than their historical baseline (warehouse-wide) |

## Quick Start

```python
from fabric_warehouse_advisor import PerformanceCheckAdvisor, PerformanceCheckConfig

config = PerformanceCheckConfig(
    warehouse_name="MyWarehouse",
)

advisor = PerformanceCheckAdvisor(spark, config)
result = advisor.run()

# Rich HTML report
displayHTML(result.html_report)
```

## Output Model

Unlike the Data Clustering advisor (which produces scores), the
Performance Check advisor produces **findings** at five severity levels:

| Level | Meaning |
|-------|--------|
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

### Summary Counts

```python
# Summary counts by severity
print(f"Critical: {result.critical_count}")
print(f"High:     {result.high_count}")
print(f"Medium:   {result.medium_count}")
print(f"Low:      {result.low_count}")
print(f"Info:     {result.info_count}")
```

### Iterating Findings

```python
# Iterate findings (sorted by severity)
for f in result.findings:
    print(f"[{f.level}] {f.object_name}: {f.message}")

# Filter to actionable findings only (excludes INFO)
for f in result.findings:
    if f.is_actionable:
        print(f"[{f.level}] {f.object_name}: {f.message}")
        if f.recommendation:
            print(f"  → {f.recommendation}")
```

### Filtering by Category or Level

```python
# Only critical findings
critical = [f for f in result.findings if f.is_critical]
display(dt_findings)

# Only data type findings
from fabric_warehouse_advisor.advisors.performance_check.findings import CATEGORY_DATA_TYPES

dt_findings = result.summary.findings_by_category(CATEGORY_DATA_TYPES)
display(dt_findings)
```

### Saving Reports

```python
result.save("/lakehouse/default/Files/reports/perf_report.html")
result.save("/lakehouse/default/Files/reports/perf_report.md", "md")
result.save("/lakehouse/default/Files/reports/perf_report.txt", "txt")
```

## Documentation

| Document | Description |
|----------|-------------|
| [How It Works](how-it-works.md) | The multi-phase pipeline |
| [Configuration](configuration.md) | Full parameter reference (~40 config fields) |
| [Checks Reference](checks.md) | Deep dive into each check category |
| [Reports](reports.md) | Text, Markdown, and HTML report formats |
