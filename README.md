# Fabric Warehouse Advisor
 
A modular Python **advisory framework** for **Microsoft Fabric Warehouse**.
Each advisor module analyses a different aspect of warehouse health and
produces scored recommendations with rich reports.

  ## Available Advisors
  
  | Advisor | What it does | Output |
  |---------|-------------|--------|
  | [**Data Clustering**](https://tiagobalabuch.github.io/fabric-warehouse-advisor/advisors/data-clustering/) | Recommends which tables and columns should use `CLUSTER BY` | Scored recommendations (0–100) with CTAS DDL |
  | [**Performance Check**](https://tiagobalabuch.github.io/fabric-warehouse-advisor/advisors/performance-check/) | Detects data-type, query regression, caching misconfigurations, V-Order status, and statistics health problems | Findings (Critical / Warning / Info) |
  | [**Security Check**](https://tiagobalabuch.github.io/fabric-warehouse-advisor/advisors/security-check/) | Analyses permissions, roles, RLS, CLS, and Dynamic Data Masking configuration | Findings (Critical / High / Medium / Low / Info) |

It runs entirely inside a **Fabric Notebook**. The Microsoft Fabric Data Warehouse connector comes pre-installed in the Fabric runtime, and Query Insights is enabled by default on every warehouse. A Lakehouse is required only when the solution is installed from a wheel file stored in OneLake.

## Installation

To install Fabric Warehouse Advisor, run:

```python
%pip install fabric-warehouse-advisor
```

For version information, dependencies, and release notes, see the [details](https://github.com/tiagobalabuch/fabric-warehouse-advisor/blob/master/CHANGELOG.md).

## Quick Start

### Data Clustering

```python
from fabric_warehouse_advisor import DataClusteringAdvisor, DataClusteringConfig

config = DataClusteringConfig(
    warehouse_name="MyWarehouse",
)

advisor = DataClusteringAdvisor(spark, config)
result = advisor.run()

# Rich HTML report — best way to view results in a Fabric notebook
displayHTML(result.html_report)
```

### Performance Check

```python
from fabric_warehouse_advisor import PerformanceCheckAdvisor, PerformanceCheckConfig

config = PerformanceCheckConfig(
    warehouse_name="MyWarehouse",
)

advisor = PerformanceCheckAdvisor(spark, config)
result = advisor.run()

displayHTML(result.html_report)
```

### Security Check

```python
from fabric_warehouse_advisor import SecurityCheckAdvisor, SecurityCheckConfig

config = SecurityCheckConfig(
    warehouse_name="MyWarehouse",
)

advisor = SecurityCheckAdvisor(spark, config)
result = advisor.run()

displayHTML(result.html_report)
```

## Screenshots

Each advisor produces a rich, interactive HTML report with light and dark themes.

### Data Clustering

<p>
  <img src="https://raw.githubusercontent.com/tiagobalabuch/fabric-warehouse-advisor/master/docs/assets/screenshots/data-clustering-light.png" alt="Data Clustering - Light" width="49%">
  
  <img src="https://raw.githubusercontent.com/tiagobalabuch/fabric-warehouse-advisor/master/docs/assets/screenshots/data-clustering-dark.png" alt="Data Clustering - Dark" width="49%">
</p>

### Security Check

<p>
  <img src="https://raw.githubusercontent.com/tiagobalabuch/fabric-warehouse-advisor/master/docs/assets/screenshots/security-check-light.png" alt="Security Check - Light" width="49%">

  <img src="https://raw.githubusercontent.com/tiagobalabuch/fabric-warehouse-advisor/master/docs/assets/screenshots/security-check-dark.png" alt="Security Check - Dark" width="49%">
</p>

### Performance Check

<p>
  <img src="https://raw.githubusercontent.com/tiagobalabuch/fabric-warehouse-advisor/master/docs/assets/screenshots/performance-check-light.png" alt="Performance Check - Light" width="49%">
  
  <img src="https://raw.githubusercontent.com/tiagobalabuch/fabric-warehouse-advisor/master/docs/assets/screenshots/performance-check-dark.png" alt="Performance Check - Dark" width="49%">
</p>

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](https://tiagobalabuch.github.io/fabric-warehouse-advisor/getting-started/) | Installation, first run, working with results |
| [Advisors Overview](https://tiagobalabuch.github.io/fabric-warehouse-advisor/advisors/) | Comparison of all available advisors |
| **Data Clustering** | |
| [Overview](https://tiagobalabuch.github.io/fabric-warehouse-advisor/advisors/data-clustering/#documentation) | Analyzes query patterns, table metadata, and column cardinality to identify and score the best candidate columns for data clustering, optimizing physical data organization on OneLake for better query speed. |
| **Performance Check** | |
| [Overview](https://tiagobalabuch.github.io/fabric-warehouse-advisor/advisors/performance-check/) | Identifies common performance pitfalls in Fabric Warehouses and Lakehouse SQL Endpoints by auditing data types, caching status, V-Order optimization, statistics health, and query performance regressions. |
| **Security Check** | |
| [Overview](https://tiagobalabuch.github.io/fabric-warehouse-advisor/advisors/security-check/) | Scans Microsoft Fabric Warehouses for security misconfigurations, covering schema permissions, custom roles, Row-Level Security (RLS), Column-Level Security (CLS), and Dynamic Data Masking to provide actionable findings and SQL fixes. |

## Acknowledgements

Report icons provided by [Flaticon](https://www.flaticon.com/):

- [Cyber security icons created by Freepik - Flaticon](https://www.flaticon.com/free-icons/cyber-security)
- [Performance icons created by Freepik - Flaticon](https://www.flaticon.com/free-icons/performance)
- [Graph icons created by Karacis - Flaticon](https://www.flaticon.com/free-icons/graph)

## License

MIT — see [LICENSE](LICENSE) for details.
