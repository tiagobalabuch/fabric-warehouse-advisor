# Available Advisors

The Fabric Warehouse Advisor ships with modular advisor modules.
Each one analyses a different aspect of warehouse health and produces
its own reports.

## Advisor Comparison

| | Data Clustering | Performance Check | Security Check |
|--|----------------|-------------------|----------------|
| **Purpose** | Recommend optimal `CLUSTER BY` columns | Detect performance anti-patterns | Detect security misconfigurations |
| **Output model** | Scored recommendations (0–100) | Findings (Critical / High / Medium / Low / Info) | Findings (Critical / High / Medium / Low / Info) |
| **Applies to** | DataWarehouse only | DataWarehouse and Lakehouse SQL Endpoints | DataWarehouse and Lakehouse SQL Endpoints |
| **Config class** | `DataClusteringConfig` | `PerformanceCheckConfig` | `SecurityCheckConfig` |
| **Result class** | `DataClusteringResult` | `PerformanceCheckResult` | `SecurityCheckResult` |
| **Reports** | Text, Markdown, HTML | Text, Markdown, HTML | Text, Markdown, HTML |
| **DDL generation** | Yes (CTAS statements) | Yes (per-finding SQL fixes) | Yes (per-finding SQL fixes) |
| **Phases** | 7 (metadata → scoring) | 7 (edition → statistics) | 5 (permissions → DDM) |


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

## Security Check Advisor

Scans your warehouse for security misconfigurations across permissions, roles, Row‑Level Security, Column‑Level Security, and Dynamic Data Masking.

```python
from fabric_warehouse_advisor import SecurityCheckAdvisor, SecurityCheckConfig

config = SecurityCheckConfig(warehouse_name="MyWarehouse")
result = SecurityCheckAdvisor(spark, config).run()
displayHTML(result.html_report)
```

[Full documentation →](security-check/index.md)

## Running All Advisors Together

For a comprehensive warehouse health assessment, run all advisors
in the same notebook:

```python
from fabric_warehouse_advisor import (
    DataClusteringAdvisor, DataClusteringConfig,
    PerformanceCheckAdvisor, PerformanceCheckConfig,
    SecurityCheckAdvisor, SecurityCheckConfig,
)

warehouse = "MyWarehouse"

# Performance check first — fix anti-patterns before optimizing clustering
pc_result = PerformanceCheckAdvisor(spark, PerformanceCheckConfig(
    warehouse_name=warehouse,
)).run()

# Security check — identify access-control gaps
sc_result = SecurityCheckAdvisor(spark, SecurityCheckConfig(
    warehouse_name=warehouse,
)).run()

dc_result = DataClusteringAdvisor(spark, DataClusteringConfig(
    warehouse_name=warehouse,
)).run()

# Display all reports
displayHTML(pc_result.html_report)
displayHTML(sc_result.html_report)
displayHTML(dc_result.html_report)
```

!!! tip "Recommended order"
    Run the **Performance Check** first — resolving data-type issues,
    enabling caching, and fixing statistics will improve the accuracy
    of the Data Clustering advisor's recommendations.
