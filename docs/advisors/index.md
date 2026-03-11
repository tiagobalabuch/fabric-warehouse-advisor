# Available Advisors

The Fabric Warehouse Advisor ships with modular advisor modules.
Each one analyses a different aspect of warehouse health and produces
its own reports.

## Advisor Comparison

| | Data Clustering | Performance Check |
|--|----------------|-------------------|
| **Purpose** | Recommend optimal `CLUSTER BY` columns | Detect performance anti-patterns |
| **Output model** | Scored recommendations (0–100) | Findings (Critical / High / Medium / Low / Info) |
| **Applies to** | DataWarehouse only | DataWarehouse and Lakehouse SQL Endpoints |
| **Config class** | `DataClusteringConfig` | `PerformanceCheckConfig` |
| **Result class** | `DataClusteringResult` | `PerformanceCheckResult` |
| **Reports** | Text, Markdown, HTML | Text, Markdown, HTML |
| **DDL generation** | Yes (CTAS statements) | Yes (per-finding SQL fixes) |
| **Phases** | 7 (metadata → scoring) | 7 (edition → statistics) |


## Data Clustering Advisor

Analyses your **actual query patterns** (via Query Insights), combines them with **table metadata** and **column cardinality estimates**, and scores every candidate column from 0 to 100. You get a clear report telling you exactly what to cluster and why.

```python
from fabric_warehouse_advisor import DataClusteringAdvisor, DataClusteringConfig

config = DataClusteringConfig(warehouse_name="MyWarehouse")
result = DataClusteringAdvisor(spark, config).run()
displayHTML(result.html_report)
```

[Full documentation →](data-clustering/index.md)

## Performance Check Advisor

Scans for common performance pitfalls across multiple categories, including warehouse edition detection, data‑type anti‑patterns, caching configuration, collation settings, query regression, statistics health, and V‑Order optimization state.

!!! info "Checking Warehouse edition"
    Not all checks apply to every warehouse edition.

```python
from fabric_warehouse_advisor import PerformanceCheckAdvisor, PerformanceCheckConfig

config = PerformanceCheckConfig(warehouse_name="MyWarehouse")
result = PerformanceCheckAdvisor(spark, config).run()
displayHTML(result.html_report)
```

[Full documentation →](performance-check/index.md)

## Running Both Advisors Together

For a comprehensive warehouse health assessment, run both advisors
in the same notebook:

```python
from fabric_warehouse_advisor import (
    DataClusteringAdvisor, DataClusteringConfig,
    PerformanceCheckAdvisor, PerformanceCheckConfig,
)

warehouse = "MyWarehouse"

# Performance check first — fix anti-patterns before optimizing clustering
pc_result = PerformanceCheckAdvisor(spark, PerformanceCheckConfig(
    warehouse_name=warehouse,
)).run()

dc_result = DataClusteringAdvisor(spark, DataClusteringConfig(
    warehouse_name=warehouse,
)).run()

# Display both reports
displayHTML(pc_result.html_report)
displayHTML(dc_result.html_report)
```

!!! tip "Recommended order"
    Run the **Performance Check** first — resolving data-type issues,
    enabling caching, and fixing statistics will improve the accuracy
    of the Data Clustering advisor's recommendations.
