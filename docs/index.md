# Fabric Warehouse Advisor

A **modular Python advisory framework** for **Microsoft Fabric Warehouse**. Each advisor module analyses a different aspect of warehouse health and produces actionable recommendations with rich reports.

Everything runs inside a **Fabric Notebook** — no external tools, no data leaves your environment.

## Available Advisors

| Advisor | What it does | Output |
|---------|-------------|--------|
| [**Data Clustering**](advisors/data-clustering/index.md) | Recommends which tables and columns should use `CLUSTER BY` | Scored recommendations (0–100) with CTAS DDL |
| [**Performance Check**](advisors/performance-check/index.md) | Detects data-type, query regression, caching misconfigurations, V-Order status, and statistics health problems | Findings (Critical / Warning / Info) |
| [**Security Check**](advisors/security-check/index.md) | Analyses permissions, roles, RLS, CLS, and Dynamic Data Masking configuration | Findings (Critical / High / Medium / Low / Info) |

## Why use it?

- **Data-driven decisions** — recommendations are based on your real
  workload, not rules of thumb
- **Zero setup** — Query Insights is enabled by default on every Fabric
  Warehouse; just install the library and run
- **Non-invasive** — read-only analysis via T-SQL passthrough; nothing is
  modified in your warehouse
- **Rich output** — interactive HTML reports, Markdown, plain text, and
  Spark DataFrames you can persist to Delta for tracking over time
- **Cross-workspace support** — analyse warehouses in other Fabric
  workspaces from a single notebook
- **Fully configurable** — every threshold, toggle, and weight is
  exposed as a dataclass field

## Quick Start

```python
from fabric_warehouse_advisor import (
    DataClusteringAdvisor, DataClusteringConfig,
    PerformanceCheckAdvisor, PerformanceCheckConfig,
    SecurityCheckAdvisor, SecurityCheckConfig,
)

# --- Data Clustering ---
dc_config = DataClusteringConfig(warehouse_name="MyWarehouse")
dc_result = DataClusteringAdvisor(spark, dc_config).run()
displayHTML(dc_result.html_report)

# --- Performance Check ---
pc_config = PerformanceCheckConfig(warehouse_name="MyWarehouse")
pc_result = PerformanceCheckAdvisor(spark, pc_config).run()
displayHTML(pc_result.html_report)

# --- Security Check ---
sc_config = SecurityCheckConfig(warehouse_name="MyWarehouse")
sc_result = SecurityCheckAdvisor(spark, sc_config).run()
displayHTML(sc_result.html_report)
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
| [Getting Started](getting-started.md) | Installation, first run, prerequisites |
| **Advisors** | [Overview](advisors/index.md) |
| &nbsp;&nbsp;[Data Clustering](advisors/data-clustering/index.md) | Overview, pipeline, configuration, scoring, reports |
| &nbsp;&nbsp;[Performance Check](advisors/performance-check/index.md) | Overview, pipeline, configuration, checks reference, reports |
| &nbsp;&nbsp;[Security Check](advisors/security-check/index.md) | Overview, pipeline, configuration, checks reference, reports |
| [Cross-Workspace](cross-workspace.md) | Analysing warehouses in other workspaces |
| [Troubleshooting](troubleshooting.md) | Common issues and solutions |

## Acknowledgements

Report icons provided by [Flaticon](https://www.flaticon.com/):

- [Cyber security icons created by Freepik - Flaticon](https://www.flaticon.com/free-icons/cyber-security)
- [Performance icons created by Freepik - Flaticon](https://www.flaticon.com/free-icons/performance)
- [Graph icons created by Karacis - Flaticon](https://www.flaticon.com/free-icons/graph)

## License

MIT — see [LICENSE](https://github.com/tiagobalabuch/fabric-warehouse-advisor/blob/master/LICENSE) for details.
